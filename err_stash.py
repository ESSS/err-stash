import argparse
from collections import OrderedDict
from configparser import ConfigParser
from pathlib import Path

import stashy
from errbot import BotPlugin, botcmd, arg_botcmd


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


class MergePlan:
    """
    Contains information about branch and PRs that will be involved in a merge operation.
    """
    def __init__(self, project, slug):
        self.project = project
        self.slug = slug
        self.branches = []
        self.pull_requests = []


def get_self_url(d):
    """Returns the URL of a Stash resource"""
    return d['links']['self'][0]['href']


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


def create_plans(api, projects, branch_text):
    """
    Go over all the branches in all repositories searching for branches and PRs that match the given branch text.

    :rtype: List[MergePlan]
    """
    repos = []
    for project in projects:
        repos += api.fetch_repos(project)
    plans = []
    has_prs = False
    for repo in repos:
        slug = repo['slug']
        project = repo['project']['key']
        branches = list(api.fetch_branches(project, slug, branch_text))
        if branches:
            plan = MergePlan(project, slug)
            plans.append(plan)
            plan.branches = branches
            branch_ids = [x['id'] for x in plan.branches]
            prs = list(api.fetch_pull_requests(project, slug))
            for pr in prs:
                has_prs = True
                if pr['fromRef']['id'] in branch_ids:
                    plan.pull_requests.append(pr)

    if not plans:
        raise CheckError('Could not find any branch with text `"{}"` in any repositories of projects {}.'.format(
            branch_text,
            ', '.join('`{}`'.format(x) for x in projects),
        ))

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
            names = ', '.join('`{}`'.format(x['displayId']) for x in plan.branches)
            error_lines.append("`{slug}`: {names}".format(slug=plan.slug, names=names))

    if error_lines:
        error_lines.append("Use a more complete text or remove one of the branches.")
        raise CheckError(error_lines)

    return plans[0].branches[0]['displayId']


def ensure_unique_pull_requests(plans, from_branch_display_id):
    """Ensure we have only one PR per repository for the given branch"""
    error_lines = []
    for plan in plans:
        if len(plan.pull_requests) > 1:
            if not error_lines:
                error_lines.append('Multiples PRs for branch `{}` found:'.format(from_branch_display_id))
            links = ['[PR#{id}]({url})'.format(id=x['id'], url=get_self_url(x)) for x in plan.pull_requests]
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
                result = plan.pull_requests[0]['toRef']['id']
            elif result != plan.pull_requests[0]['toRef']['id']:
                multiple_target_branches = True
                break

    if multiple_target_branches:
        error_lines = ['PRs in repositories for branch `{}` have different targets:'.format(from_branch_display_id)]
        for plan in plans:
            if plan.pull_requests:
                assert len(plan.pull_requests) == 1
                pr = plan.pull_requests[0]
                error_lines.append('`{slug}`: [PR#{id}]({url}) targets `{to_ref}`'.format(
                                   slug=plan.slug, id=pr['id'], url=get_self_url(pr), to_ref=pr['toRef']['id']))

        error_lines.append('Fix those PRs and try again.')
        raise CheckError(error_lines)

    return result


def make_pr_link(api, project, slug, from_branch, to_branch):
    """Generates a URL that can be used to create a PR"""
    from urllib.parse import urlencode
    params = OrderedDict([('sourceBranch', from_branch), ('targetBranch', to_branch)])
    base_url = '{url}/projects/{project}/repos/{slug}/compare/commits?'.format(url=api.url, project=project,
                                                                               slug=slug)
    return base_url + urlencode(params)


def get_commits_about_to_be_merged_by_pull_requests(api, plans, from_branch, to_branch):
    """Returns a summary of the commits in each PR that will be merged"""
    error_lines = []
    result = []
    for plan in plans:
        commits = list(api.fetch_repo_commits(plan.project, plan.slug, from_branch, to_branch))
        if commits and not plan.pull_requests:
            if not error_lines:
                error_lines.append('These repositories have commits in `{}` but no PRs:'.format(from_branch))
            pr_link = make_pr_link(api, plan.project, plan.slug, from_branch, to_branch)
            error_lines.append('`{slug}`: **{commits_text}** ([create PR]({pr_link}))'.format(
                slug=plan.slug, commits_text=commits_text(commits), pr_link=pr_link))
        if commits:
            result.append((plan, commits))

    if error_lines:
        error_lines.append('You need to create PRs for your changes before merging this branch.')
        raise CheckError(error_lines)

    return result


def ensure_no_conflicts(api, from_branch, plans):
    """Ensures that all PRs are not in a conflicting state"""
    error_lines = []
    for plan in plans:
        pr_data = plan.pull_requests[0]
        pull_request = api.fetch_pull_request(plan.project, plan.slug, pr_data['id'])
        if not pull_request.can_merge():
            if not error_lines:
                error_lines.append('The PRs below for branch `{}` have conflicts:'.format(from_branch))
            error_lines.append('`{slug}`: [PR#{id}]({url}) **CONFLICTS**'.format(
                slug=plan.slug, id=pr_data['id'], url=get_self_url(pr_data)))

    if error_lines:
        error_lines.append('Fix the conflicts and try again. :wink:')
        raise CheckError(error_lines)


def merge(url, projects, username, password, branch_text, confirm):
    """
    Merges PRs in repositories which match a given branch name, performing various checks beforehand.

    :param str url: URL to stash server.
    :param list[str] projects: List of Stash project keys to search branches
    :param str username: username
    :param str password: password or access token (write access).
    :param str branch_text: complete or partial branch name to search for
    :param bool confirm: if True, perform the merge, otherwise just print what would happen.
    :raise CheckError: if a check for merging-readiness fails.
    """
    api = StashAPI(url, username=username, password=password)

    plans = create_plans(api, projects, branch_text)
    from_branch = ensure_text_matches_unique_branch(plans, branch_text)
    ensure_unique_pull_requests(plans, from_branch)
    to_branch_id = ensure_pull_requests_target_same_branch(plans, from_branch)
    assert to_branch_id, 'No pull requests! What to do?'
    plans_and_commits = get_commits_about_to_be_merged_by_pull_requests(api, plans, from_branch, to_branch_id)
    ensure_no_conflicts(api, from_branch, [plan for (plan, _) in plans_and_commits])

    yield 'Branch `{}` merged into `{}`! :white_check_mark:'.format(from_branch, to_branch_id)
    shown = set()
    for plan, commits in plans_and_commits:
        pull_request = api.fetch_pull_request(plan.project, plan.slug, plan.pull_requests[0]['id'])
        if confirm:
            # https://confluence.atlassian.com/bitbucketserverkb/bitbucket-server-rest-api-for-merging-pull-request-fails-792309002.html
            pull_request.merge(version=plan.pull_requests[0]['version'])
        yield ':white_check_mark: `{}` **{}**'.format(plan.slug, commits_text(commits))
        shown.add(plan.slug)
    other_plans = (p for p in plans if p.slug not in shown)
    for plan in other_plans:
        yield '`{}` - (no changes)'.format(plan.slug)

    for plan in plans:
        if confirm:
            api.delete_branch(plan.project, plan.slug, plan.branches[0]['id'])
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
        }

    def load_user_settings(self, user):
        key = 'user:{}'.format(user)
        settings = {
            'token': '',
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
    def stash_version(self, msg, args):
        """Get current version and CHANGELOG"""
        return Path(__file__).parent.joinpath('CHANGELOG.md').read_text()


    @botcmd(split_args_with=None)
    def stash_token(self, msg, args):
        """Set or get your Stash token"""
        user = msg.frm.nick
        settings = self.load_user_settings(user)
        if not args:
            if settings['token']:
                return "You API Token is: `{}` (user: {})".format(settings['token'], user)
            else:
                return NO_TOKEN_MSG.format(stash_url=self.config['STASH_URL'])
        else:
            settings['token'] = args[0]
            self.save_user_settings(user, settings)
            return "Token saved."


    @arg_botcmd('branch_text', help='Branch name to merge')
    def stash_merge(self, msg, branch_text):
        """Merges PRs related to a branch (which can be a partial match)"""
        user = msg.frm.nick
        settings = self.load_user_settings(user)
        if not settings['token']:
            return self.stash_token(msg, [])
        projects = self.config['STASH_PROJECTS']
        if not projects:
            return '`STASH_PROJECTS` not configured. Use `!plugin config Stash` to configure it.'
        try:
            lines = list(merge(self.config['STASH_URL'], projects, username=user, password=settings['token'],
                               branch_text=branch_text, confirm=True))
        except CheckError as e:
            lines = e.lines
        return '\n'.join(lines)


NO_TOKEN_MSG = """
**Stash API Token not configured**. 
Create a new token [here]({stash_url}/plugins/servlet/access-tokens/manage) with **write access** and then execute:
    `!stash token <TOKEN>` 
This only needs to be done once.
"""


def main(args):
    """Command-line implementation.

    For convenience one can define a "default.ini" file with user name and token:

    [err-stash]
    user = bruno
    password = secret-token
    """
    p = Path(__file__).parent.joinpath('default.ini')
    if p.is_file():
        config = ConfigParser()
        config.read(str(p))

        default_user = config['err-stash']['user']
        default_password = config['err-stash']['password']
    else:
        default_user = None
        default_password = None

    parser = argparse.ArgumentParser(description='Merge multiples branches.')
    parser.add_argument('-u', '--username', default=default_user)
    parser.add_argument('-p', '--password', default=default_password)
    parser.add_argument('--confirm', default=False, action='store_true')
    parser.add_argument('text', help='Branch text (possibly partial) to search for')
    parser.add_argument('projects', help='list of Stash projects to search branches, separated by commas')

    options = parser.parse_args(args)
    try:
        lines = list(merge("https://eden.esss.com.br/stash", options.projects.split(','), username=options.username,
                           password=options.password, branch_text=options.text, confirm=options.confirm))
        result = 0
    except CheckError as e:
        lines = e.lines
        result = 4
    print('\n'.join(lines))
    return result


if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))
