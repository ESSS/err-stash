import argparse
import logging
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from configparser import ConfigParser
from pathlib import Path

import stashy
import stashy.errors
from errbot import BotPlugin, botcmd, arg_botcmd
from github import Github, GithubException
from github.Branch import Branch
from github.PullRequest import PullRequest


class StashAPI:
    """
    Thin access to the stashy API.

    We have this thin layer in order to mock it during testing.
    """

    def __init__(self, url, *, username, password):
        self._stash = stashy.connect(url, username=username, password=password)
        self._url = url

    @property
    def url(self):
        return self._url

    def fetch_repos(self, project):
        return self._stash.projects[project].repos.list()

    def fetch_branches(self, project, slug, filter_text):
        return self._stash.projects[project].repos[slug].branches(filterText=filter_text)

    def delete_branch(self, project, slug, branch):
        return self._stash.projects[project].repos[slug].delete_branch(branch)

    def fetch_pull_requests(self, project, slug):
        return self._stash.projects[project].repos[slug].pull_requests.all()

    def fetch_pull_request(self, project, slug, pr_id):
        return self._stash.projects[project].repos[slug].pull_requests[pr_id]

    def fetch_repo_commits(self, project, slug, until, since):
        return self._stash.projects[project].repos[slug].commits(until, since)


class GithubAPI:
    """
    Access to the pygithub API.
    """
    def __init__(self, login_or_token=None, password=None, organizations=[]):
        self._github = Github(login_or_token=login_or_token, password=password)
        self.url = 'https://github.com'
        # disable PyGithub logger
        logging.disable(logging.CRITICAL)

        # organizations cache
        self.organizations = {
            organization: self._github.get_organization(organization) for organization in organizations}

        self.repos = dict()
        # repositories cache
        for organization in self.organizations:
            repos = list(self.organizations[organization].get_repos())
            self.repos[organization] = {repo.name: repo for repo in repos}

    def fetch_repos(self, organization:str):
        return list(self.repos[organization].values())

    def fetch_branches(self, organization:str, repo_name:str, *, branch_name:str=''):
        """
        Returns a list of branches based on organization and name of the repository.

        :param str organization:
            Name of the Github organization.

        :param str repo_name:
            Name of the repository.

        :param branch_name:
            If passed, searches for a specific branch, otherwise all branches of the repository are returned
        """
        repo = self.repos[organization][repo_name]
        if branch_name == '':
            return list(repo.get_branches())

        try:
            return [repo.get_branch(branch_name)]
        except GithubException as e:
            if e.status == 404:  # branch doesn't exist on this repo
                # REMINDER: trying to match branch name like this:
                # return [branch for branch in list(repo.get_branches()) if branch_name in branch.name]
                # slows down the merge plans creation in ~5 to ~10 extra seconds.
                return []
            else:
                raise

    def delete_branch(self, organization:str, repo_name:str, branch_name:str, pr_id):
        """
        Deletes the branch based on the pr_id, then Github closes the PR.
        """
        repo = self.repos[organization][repo_name]
        git_ref = repo.get_git_ref("heads/{ref}".format(ref=repo.get_pull(pr_id).head.ref))
        assert branch_name in git_ref.ref, "Trying to delete the wrong branch, check the PR ID."
        git_ref.delete()

    def fetch_pull_requests(self, organization, repo_name):
        """
        Returns a list with all open pull requests
        """
        return list(self.repos[organization][repo_name].get_pulls())

    def fetch_pull_request(self, organization, repo_name, pr_id):
        """
        Returns a specific github.PullRequest.PullRequest

        :param str organization:
            Name of the Github organization

        :param str repo_name:
            Name of the Github repository

        :param int pr_id:
            Pull request ID (the same you see on the Github PR page).
        """
        return self.repos[organization][repo_name].get_pull(pr_id)

    def fetch_repo_commits(self, organization, repo_name, from_branch:str, to_branch:str):
        """
        Returns a list of commits that are on from_branch, but not in to_branch, i.e.,
        commits that are added by the PR.

        :param from_branch:
             Name of the branch that created the PR (Github calls this head branch).

        :param to_branch:
            Name of the target branch in the PR (Github calls this base branch).
        """
        repo = self.repos[organization][repo_name]
        return repo.compare(to_branch, from_branch).commits


class MergePlan:
    """
    Contains information about branch and PRs that will be involved in a merge operation.
    """

    def __init__(self, project, slug, *, comes_from_github=False):
        self.project = project
        self.slug = slug
        self.branches = []
        self.pull_requests = []
        self.to_branch = None
        self.comes_from_github = comes_from_github


def get_self_url(d):
    """Returns the URL of a Stash resource"""
    return d.html_url if isinstance(d, PullRequest) else d['links']['self'][0]['href']


def commits_text(commits):
    """Returns text in the form 'X commits' or '1 commit'"""
    plural = 's' if len(commits) != 1 else ''
    return '{} commit{}'.format(len(commits), plural)


class CheckError(Exception):
    """Exception raised when one of the various checks done before a merge is done fails"""

    def __init__(self, lines):
        if isinstance(lines, str):
            lines = [lines]
        super().__init__('\n'.join(lines))
        self.lines = lines


def create_plans(stash_api, github_api, stash_projects, github_organizations, branch_text):
    """
    Go over all the branches in all repositories searching for branches and PRs that match the given branch text.

    :rtype: List[MergePlan]
    """
    # Plans for Stash repos:
    stash_repos = []
    for project in stash_projects:
        stash_repos += stash_api.fetch_repos(project)

    plans = []
    has_prs = False
    for repo in stash_repos:
        slug = repo['slug']
        project = repo['project']['key']
        branches = list(stash_api.fetch_branches(project, slug, branch_text))
        if branches:
            plan = MergePlan(project, slug)
            plans.append(plan)
            plan.branches = branches
            branch_ids = [x['id'] for x in plan.branches]
            prs = list(stash_api.fetch_pull_requests(project, slug))
            for pr in prs:
                has_prs = True
                if pr['fromRef']['id'] in branch_ids:
                    plan.pull_requests.append(pr)
            if plan.pull_requests:
                plan.to_branch = plan.pull_requests[0]['toRef']['id']

    # Plans for Github repos:
    github_repos = []
    github_branch_text = branch_text
    if len(plans) > 0:
        # if we already found the branch name on Stash, we can use its name here
        branch_id = plans[0].branches[0]['id']
        if 'refs/heads/' in branch_id:
            branch_id = branch_id[len('refs/heads/'):]

        github_branch_text = branch_id

    organization_to_repos = defaultdict(list)
    for organization in github_organizations:
        organization_to_repos[organization] += github_api.fetch_repos(organization)

    futures = dict()
    for organization in organization_to_repos.keys():
        with ThreadPoolExecutor(max_workers=16) as executor:
            for repo in organization_to_repos[organization]:
                repo_name = repo.name
                f = executor.submit(
                    github_api.fetch_branches, organization, repo_name, branch_name=github_branch_text)

                futures[f] = repo_name

            for f in as_completed(futures.keys()):
                branches = f.result()
                if not branches:
                    continue

                plan = MergePlan(repo.owner.name, repo.name, comes_from_github=True)
                plans.append(plan)
                plan.branches = branches

                prs = github_api.fetch_pull_requests(organization, repo_name)

                for pr in prs:
                    if pr.head.ref == github_branch_text:
                        has_prs = True
                        plan.pull_requests.append(pr)
                if plan.pull_requests:
                    plan.to_branch = 'refs/heads/{}'.format(plan.pull_requests[0].base.ref)

    if not plans:
        raise CheckError(
            'Could not find any branch with text `"{}"` in any repositories of Stash projects: {} nor '
            'Github organizations: {}.'.format(
                branch_text,
                ', '.join('`{}`'.format(x) for x in stash_projects),
                ', '.join('`{}`'.format(x) for x in github_organizations),)
        )

    if not has_prs:
        raise CheckError('No PRs are open with text `"{}"`'.format(branch_text))
    return plans


def ensure_text_matches_unique_branch(plans, branch_text):
    """Ensure that the given branch text matches only a single branch"""
    # check if any of the plans have matched more than one branch
    error_lines = []
    for plan in plans:
        if len(plan.branches) > 1:
            if not error_lines:
                error_lines.append('More than one branch matches the text `"{}"`:'.format(branch_text))
            names = ', '.join('`{}`'.format(
                x.name if isinstance(x, Branch) else x['displayId']) for x in plan.branches)
            error_lines.append("`{slug}`: {names}".format(slug=plan.slug, names=names))

    if error_lines:
        error_lines.append("Use a more complete text or remove one of the branches.")
        raise CheckError(error_lines)

    branch = plans[0].branches[0]
    return branch.name if isinstance(branch, Branch) else branch['displayId']


def ensure_unique_pull_requests(plans, from_branch_display_id):
    """Ensure we have only one PR per repository for the given branch"""
    error_lines = []
    for plan in plans:
        if len(plan.pull_requests) > 1:
            if not error_lines:
                error_lines.append('Multiples PRs for branch `{}` found:'.format(from_branch_display_id))
            links = ['[PR#{id}]({url})'.format(
                id=x.number if isinstance(x, PullRequest) else x['id'],
                url=get_self_url(x))
                for x in plan.pull_requests
            ]
            error_lines.append("`{slug}`: {links}".format(slug=plan.slug, links=', '.join(links)))
    if error_lines:
        error_lines.append('Sorry you will have to sort that mess yourself. :wink:')
        raise CheckError(error_lines)


def ensure_pull_requests_target_same_branch(plans, from_branch_display_id):
    """Ensure that all PRs target the same branch"""
    # check that all PRs for the branch target the same "to" branch
    result = None
    multiple_target_branches = False
    for plan in plans:
        if plan.pull_requests:
            assert len(plan.pull_requests) == 1
            if result is None:
                result = plan.to_branch
            elif result != plan.to_branch:
                multiple_target_branches = True
                break

    if multiple_target_branches:
        error_lines = ['PRs in repositories for branch `{}` have different targets:'.format(from_branch_display_id)]
        for plan in plans:
            if plan.pull_requests:
                assert len(plan.pull_requests) == 1
                pr = plan.pull_requests[0]
                error_lines.append('`{slug}`: [PR#{id}]({url}) targets `{to_ref}`'.format(
                    slug=plan.slug,
                    id=pr.number if isinstance(pr, PullRequest) else pr['id'],
                    url=get_self_url(pr),
                    to_ref=pr.base.ref if isinstance(pr, PullRequest) else pr['toRef']['id'])
                )

        error_lines.append('Fix those PRs and try again. ')
        error_lines.append('Alternately you can pass `--force` to force the merge with different targets!')
        raise CheckError(error_lines)


def make_pr_link(url, project, slug, from_branch, to_branch):
    """Generates a URL that can be used to create a PR"""
    from urllib.parse import urlencode
    if 'github' in url:
        result = '{url}/{organization}/{repo_name}/compare/{from_branch}'.format(
            url=url, organization=project, repo_name=slug, from_branch=from_branch)
    else:
        base_url = '{url}/projects/{project}/repos/{slug}/compare/commits?'.format(
            url=url, project=project, slug=slug)
        result = base_url + urlencode(
            OrderedDict([('sourceBranch', from_branch), ('targetBranch', to_branch)]))

    return result


def get_commits_about_to_be_merged_by_pull_requests(stash_api, github_api, plans, from_branch):
    """Returns a summary of the commits in each PR that will be merged"""
    error_lines = []
    result = []
    default_branch = next(plan.to_branch for plan in plans if plan.to_branch)

    def default_branch_exists(plan):
        branch_name = default_branch.replace("refs/heads/", "")

        if plan.comes_from_github:
            branches = list(github_api.fetch_branches(
                organization=plan.project, repo_name=plan.slug, branch_name=branch_name))
        else:
            branches = list(stash_api.fetch_branches(plan.project, plan.slug, branch_name))

        return len(branches) > 0

    for plan in plans:
        if plan.to_branch:
            to_branch = plan.to_branch
        elif default_branch_exists(plan):
            to_branch = default_branch
        else:
            to_branch = "refs/heads/master"

        if plan.comes_from_github:
            commits = list(github_api.fetch_repo_commits(
                organization=plan.project,
                repo_name=plan.slug,
                from_branch=from_branch,
                to_branch=to_branch
            ))
        else:
            try:
                commits = list(stash_api.fetch_repo_commits(plan.project, plan.slug, from_branch, to_branch))
            except stashy.errors.NotFoundException:
                commits = []

        if commits and not plan.pull_requests:
            if not error_lines:
                error_lines.append('These repositories have commits in `{}` but no PRs:'.format(from_branch))
            pr_link = make_pr_link(github_api.url if plan.comes_from_github else stash_api.url,
                                   plan.project,
                                   plan.slug,
                                   from_branch,
                                   to_branch)
            error_lines.append('`{slug}`: **{commits_text}** ([create PR]({pr_link}))'.format(
                slug=plan.slug, commits_text=commits_text(commits), pr_link=pr_link))
        if commits:
            result.append((plan, commits))

    if error_lines:
        error_lines.append('You need to create PRs for your changes before merging this branch.')
        raise CheckError(error_lines)

    return result


def ensure_no_conflicts(stash_api, from_branch, plans):
    """Ensures that all PRs are not in a conflicting state"""
    error_lines = []
    for plan in plans:
        pr_data = plan.pull_requests[0]
        pr_id = plan.pull_requests[0].number if plan.comes_from_github else pr_data['id']

        pull_request = pr_data if plan.comes_from_github \
            else stash_api.fetch_pull_request(plan.project, plan.slug, pr_id)

        is_mergeable = pull_request.mergeable if plan.comes_from_github else pull_request.can_merge()
        if not is_mergeable:
            if not error_lines:
                error_lines.append(
                    'The PRs below for branch `{}` have problems such as conflicts, '
                    'build requirements, etc:'.format(from_branch))

            error_lines.append('`{slug}`: [PR#{id}]({url})'.format(
                slug=plan.slug, id=pr_id, url=get_self_url(pr_data)))

    if error_lines:
        error_lines.append('Fix them and try again.')
        raise CheckError(error_lines)


def ensure_has_pull_request(plans):
    message = """
    No pull request open for this branch!
    """
    if not any(plan for plan in plans if plan.to_branch):
        raise CheckError(message)


def merge(
        url,
        stash_projects,
        stash_username,
        stash_password,
        github_username_or_token,
        github_password,
        github_organizations,
        branch_text,
        confirm,
        force=False):
    """
    Merges PRs in repositories which match a given branch name, performing various checks beforehand.

    :param str url: URL to stash server.
    :param list[str] stash_projects: List of Stash project keys to search branches
    :param str stash_username: username
    :param str stash_password: password or access token (write access).
    :param str github_username_or_token: username or token
    :param str github_password: password
    :param list github_organizations: List of organization names to search repositories
    :param str branch_text: complete or partial branch name to search for
    :param bool confirm: if True, perform the merge, otherwise just print what would happen.
    :param bool force: if True, won't check if branch target are the same
    :raise CheckError: if a check for merging-readiness fails.
    """
    stash_api = StashAPI(url, username=stash_username, password=stash_password)
    github_api = GithubAPI(
        login_or_token=github_username_or_token, password=github_password, organizations=github_organizations)

    plans = create_plans(stash_api, github_api, stash_projects, github_organizations, branch_text)
    from_branch = ensure_text_matches_unique_branch(plans, branch_text)
    ensure_unique_pull_requests(plans, from_branch)
    ensure_has_pull_request(plans)
    if not force:
        ensure_pull_requests_target_same_branch(plans, from_branch)
    plans_and_commits = get_commits_about_to_be_merged_by_pull_requests(stash_api, github_api, plans, from_branch)
    ensure_no_conflicts(stash_api, from_branch, [plan for (plan, _) in plans_and_commits])

    yield 'Branch `{}` merged into:'.format(from_branch)
    shown = set()
    for plan, commits in plans_and_commits:
        pull_request = plan.pull_requests[0] if plan.comes_from_github else \
            stash_api.fetch_pull_request(plan.project, plan.slug, plan.pull_requests[0]['id'])
        if confirm:
            if plan.comes_from_github:
                pull_request.merge()
            else:
                # https://confluence.atlassian.com/bitbucketserverkb/bitbucket-server-rest-api-for-merging-pull-request-fails-792309002.html
                pull_request.merge(version=plan.pull_requests[0]['version'])
        yield ':white_check_mark: `{}` **{}** -> `{}`'.format(plan.slug, commits_text(commits),
                                                              plan.to_branch.replace('refs/heads/', ''))
        shown.add(plan.slug)
    other_plans = (p for p in plans if p.slug not in shown)
    for plan in other_plans:
        yield '`{}` - (no changes)'.format(plan.slug)

    for plan in plans:
        if confirm:
            if plan.comes_from_github:
                github_api.delete_branch(
                    organization=plan.project,
                    repo_name=plan.slug,
                    branch_name=plan.branches[0].name,
                    pr_id=plan.pull_requests[0].number
                )
            else:
                stash_api.delete_branch(plan.project, plan.slug, plan.branches[0]['id'])
    repo_list = ['`{}`'.format(p.slug) for p in plans]
    yield 'Branch deleted from repositories: {}'.format(', '.join(repo_list))
    if not confirm:
        yield '{x} dry-run {x}'.format(x='-' * 30)


class StashBot(BotPlugin):
    """Stash commands tailored to ESSS workflow"""

    def get_configuration_template(self):
        return {
            'STASH_URL': 'https://eden.esss.com.br/stash',
            'STASH_PROJECTS': None,
            'GITHUB_ORGANIZATIONS': None,
        }

    def load_user_settings(self, user):
        key = 'user:{}'.format(user)
        settings = {
            'token': '',
            'github_token': '',
        }
        loaded = self.get(key, settings)
        settings.update(loaded)
        self.log.debug('LOAD ({}) settings: {}'.format(user, settings))
        return settings

    def save_user_settings(self, user, settings):
        key = 'user:{}'.format(user)
        self[key] = settings
        self.log.debug('SAVE ({}) settings: {}'.format(user, settings))

    @botcmd
    def version(self, msg, args):
        """Get current version and CHANGELOG"""
        return Path(__file__).parent.joinpath('CHANGELOG.md').read_text()

    @botcmd(split_args_with=None)
    def stash_token(self, msg, args):
        """Set or get your Stash token"""
        user = msg.frm.nick
        settings = self.load_user_settings(user)
        if not self.config:
            return 'Stash plugin not configured, contact an admin.'
        if not args:
            if settings['token']:
                return "Your API Token is: `{}` (user: {})".format(settings['token'], user)
            else:
                return NO_TOKEN_MSG.format(stash_url=self.config['STASH_URL'])
        else:
            settings['token'] = args[0]
            self.save_user_settings(user, settings)
            return "Token saved."

    @botcmd(split_args_with=None)
    def github_token(self, msg, args):
        """Set or get Github token"""
        user = msg.frm.nick
        settings = self.load_user_settings(user)
        if not self.config:
            return 'Plugin not configured, contact an admin.'
        if not args:
            if settings['github_token']:
                return "Your Github Token is: `{}` (user: {})".format(settings['github_token'], user)
            else:
                return NO_GITHUB_TOKEN_MSG
        else:
            settings['github_token'] = args[0]
            self.save_user_settings(user, settings)
            return "Github token saved."

    @arg_botcmd('--force', action="store_true", help='If set, won\'t check target branch names')
    @arg_botcmd('branch_text', help='Branch name to merge')
    def merge(self, msg, branch_text, force=False):
        """Merges PRs related to a branch (which can be a partial match)"""
        user = msg.frm.nick
        settings = self.load_user_settings(user)
        if not settings['token']:
            return self.stash_token(msg, [])

        if not settings['github_token']:
            return self.github_token(msg, [])

        config_keys = ['STASH_PROJECTS', 'GITHUB_ORGANIZATIONS']
        for key in config_keys:
            value = self.config.get(key, None)
            if value is None or value == []:
                return '`{}` not configured. Use `!plugin config Stash` to configure it.'.format(key)

        try:
            lines = list(merge(
                url=self.config['STASH_URL'],
                stash_projects=self.config['STASH_PROJECTS'],
                stash_username=user,
                stash_password=settings['token'],
                github_password=None,
                github_username_or_token=settings['github_token'],
                github_organizations=self.config['GITHUB_ORGANIZATIONS'],
                branch_text=branch_text,
                confirm=True,
                force=force)
            )
        except CheckError as e:
            lines = e.lines
        return '\n'.join(lines)


NO_TOKEN_MSG = """
**Stash API Token not configured**. 
Create a new token [here]({stash_url}/plugins/servlet/access-tokens/manage) with **write access** and then execute:
    `!stash token <TOKEN>` 
This only needs to be done once.
"""

NO_GITHUB_TOKEN_MSG = """
**Github API Token not configured**. 
Create a new token [here](https://github.com/settings/tokens/new) with **write access** and then execute:
    `!github token <TOKEN>` 
This only needs to be done once.
"""


def main(args):
    """Command-line implementation.

    For convenience one can define a "default.ini" file with user name and token:

    [err-stash]
    user = bruno
    password = secret-token
    github_username_or_token = username-or-token
    github_password = password-if-using-username
    """
    p = Path(__file__).parent.joinpath('default.ini')
    if p.is_file():
        config = ConfigParser()
        config.read(str(p))

        default_user = config['err-stash']['user']
        default_password = config['err-stash']['password']
        default_github_username = config['err-stash']['github_username_or_token']
        default_github_password = config['err-stash']['github_password']
    else:
        default_user = None
        default_password = None
        default_github_username = None
        default_github_password = None

    parser = argparse.ArgumentParser(description='Merge multiples branches.')
    parser.add_argument('-u', '--username', default=default_user)
    parser.add_argument('-p', '--password', default=default_password)
    parser.add_argument('--github_username_or_token', default=default_github_username)
    parser.add_argument('--github_password', default=default_github_password)

    parser.add_argument('--confirm', default=False, action='store_true')
    parser.add_argument('--force', default=False, action='store_true',
                        help='Force the merge by ignoring different branches target')
    parser.add_argument('text', help='Branch text (possibly partial) to search for')
    parser.add_argument('projects', help='list of Stash projects to search branches, separated by commas')
    parser.add_argument(
        'github_organizations', help='list of Github organizations to search branches, separated by commas')

    options = parser.parse_args(args)
    try:
        lines = list(merge(
            "https://eden.esss.com.br/stash",
            options.projects.split(','),
            stash_username=options.username,
            stash_password=options.password,
            github_username_or_token=options.github_username_or_token,
            github_password=options.github_password,
            github_organizations=options.github_organizations.split(','),
            branch_text=options.text,
            confirm=options.confirm,
            force=options.force)
        )
        result = 0
    except CheckError as e:
        lines = e.lines
        result = 4
    print('\n'.join(lines))
    return result


if __name__ == '__main__':
    import sys

    sys.exit(main(sys.argv[1:]))
