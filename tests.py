import re
from collections import OrderedDict

import pytest
import stashy
import stashy.errors

from err_stash import GithubAPI, merge, StashAPI, CheckError, create_plans


class DummyPullRequest:

    def __init__(self, can_merge):
        self._can_merge = can_merge
        self.merged_version = None

    def can_merge(self):
        return self._can_merge

    def merge(self, version):
        self.merged_version = version


@pytest.fixture
def mock_stash_api(mocker):
    mocker.patch.object(stashy, 'connect', autospec=True)
    api = StashAPI('https://myserver.com/stash', username='fry', password='PASSWORD123')
    args, kwargs = stashy.connect.call_args
    assert args == ('https://myserver.com/stash',)
    assert kwargs == dict(username='fry', password='PASSWORD123')

    # noinspection PyDictCreation
    projects = {
        'PROJ-A': OrderedDict([
            ('repo1', dict(slug='repo1', project=dict(key='PROJ-A'))),
            ('repo2', dict(slug='repo2', project=dict(key='PROJ-A'))),
        ]),
        'PROJ-B': OrderedDict([
            ('repo3', dict(slug='repo3', project=dict(key='PROJ-B'))),
        ]),
    }

    projects['PROJ-A']['repo1']['branches'] = [
        dict(id='refs/heads/fb-ASIM-81-network', displayId='fb-ASIM-81-network'),
        dict(id='refs/heads/fb-SSRL-1890-py3', displayId='fb-SSRL-1890-py3'),
    ]
    projects['PROJ-A']['repo2']['branches'] = [
        dict(id='refs/heads/fb-SSRL-1890-py3', displayId='fb-SSRL-1890-py3'),
    ]
    projects['PROJ-B']['repo3']['branches'] = [
        dict(id='refs/heads/fb-ASIM-81-network', displayId='fb-ASIM-81-network'),
        dict(id='refs/heads/fb-SSRL-1890-py3', displayId='fb-SSRL-1890-py3'),
    ]

    projects['PROJ-B']['repo3']['pull_requests'] = [dict(id='10',
                                                         fromRef=dict(id='refs/heads/fb-ASIM-81-network'),
                                                         toRef=dict(id='refs/heads/master'),
                                                         displayId='fb-ASIM-81-network',
                                                         links=make_link('url.com/for/10'),
                                                         version='10')]
    projects['PROJ-B']['repo3']['pull_request'] = {
        '10': DummyPullRequest(True),
    }

    projects['PROJ-B']['repo3']['commits'] = {
        ('refs/heads/fb-ASIM-81-network', 'refs/heads/master'): ['A', 'B'],
    }

    def mock_fetch_repos(self, project):
        return projects[project].values()

    mocker.patch.object(StashAPI, 'fetch_repos', autospec=True, side_effect=mock_fetch_repos)

    def mock_fetch_branches(self, project, slug, filter_text):
        return [x for x in projects[project][slug]['branches'] if filter_text in x['id']]

    mocker.patch.object(StashAPI, 'fetch_branches', autospec=True, side_effect=mock_fetch_branches)

    def mock_fetch_pull_requests(self, project, slug):
        return projects[project][slug].get('pull_requests', [])

    mocker.patch.object(StashAPI, 'fetch_pull_requests', autospec=True, side_effect=mock_fetch_pull_requests)

    def mock_fetch_pull_request(self, project, slug, pr_id):
        return projects[project][slug].get('pull_request', {}).get(pr_id)

    mocker.patch.object(StashAPI, 'fetch_pull_request', autospec=True, side_effect=mock_fetch_pull_request)

    def mock_fetch_repo_commits(self, project, slug, from_branch, to_branch):
        for (i_from_branch, i_to_branch), commits in projects[project][slug].get('commits', {}).items():
            if from_branch in i_from_branch and to_branch in i_to_branch:
                return commits
        response = mocker.MagicMock()
        msg = 'fetch_repo_commits: {} {} {} {}'.format(project, slug, from_branch, to_branch)
        response.json.return_value = dict(errors=[dict(message=msg)])
        raise stashy.errors.NotFoundException(response=response)

    mocker.patch.object(StashAPI, 'fetch_repo_commits', autospec=True, side_effect=mock_fetch_repo_commits)

    def mock_delete_branch(self, project, slug, branch):
        branches = projects[project][slug]['branches']
        for index, item in reversed(list(enumerate(branches))):
            if branch in item['id']:
                del branches[index]

    mocker.patch.object(StashAPI, 'delete_branch', autospec=True, side_effect=mock_delete_branch)

    return projects


def call_merge(branch_text, matching_lines, force=False):
    try:
        lines = list(merge(
            'https://myserver.com/stash',
            ['PROJ-A', 'PROJ-B'],
            stash_username='fry',
            stash_password='PASSWORD123',
            github_username_or_token='',
            github_password='',
            github_organizations=[],
            branch_text=branch_text,
            confirm=True,
            force=force
        ))
    except CheckError as e:
        lines = e.lines
    from _pytest.pytester import LineMatcher
    matcher = LineMatcher(list(lines))
    matcher.re_match_lines(matching_lines)


def make_link(url):
    return dict(self=[dict(href=url)])


def test_duplicate_branches(mock_stash_api):
    mock_stash_api['PROJ-A']['repo1']['branches'] = [dict(id='refs/heads/fb-ASIM-81-network', displayId='fb-ASIM-81-network'),
                                               dict(id='refs/heads/fb-ASIM-81-network-test',
                                                    displayId='fb-ASIM-81-network-test')]
    mock_stash_api['PROJ-A']['repo2']['branches'] = [dict(id='refs/heads/fb-ASIM-81-network')]
    mock_stash_api['PROJ-B']['repo3']['branches'] = [dict(id='refs/heads/fb-ASIM-81-network'),
                                               dict(id='refs/heads/fb-SSRL-1890-py3')]

    call_merge('ASIM-81', [
        r'More than one branch matches the text `"ASIM-81"`:',
        r'`repo1`: `fb-ASIM-81-network`, `fb-ASIM-81-network-test`',
        r'Use a more complete text.*',
    ])


def test_multiples_prs_for_same_branch(mock_stash_api):
    mock_stash_api['PROJ-A']['repo1']['pull_requests'] = [dict(id='10',
                                                         fromRef=dict(id='refs/heads/fb-ASIM-81-network'),
                                                         toRef=dict(id='refs/heads/master'),
                                                         displayId='fb-ASIM-81-network',
                                                         links=make_link('url.com/for/10')),
                                                    dict(id='12',
                                                         fromRef=dict(id='refs/heads/fb-ASIM-81-network'),
                                                         toRef=dict(id='refs/heads/master'),
                                                         displayId='fb-ASIM-81-network',
                                                         links=make_link('url.com/for/12'))
                                                    ]

    call_merge('ASIM-81', [
        r'Multiples PRs for branch `fb-ASIM-81-network` found:',
        r'`repo1`: \[PR#10\]\(url.com/for/10\), \[PR#12\]\(url.com/for/12\)',
        r'Sorry you will have to sort that mess yourself. :wink:',
    ])


def test_prs_with_different_targets(mock_stash_api):
    mock_stash_api['PROJ-A']['repo1']['pull_requests'] = [dict(id='10',
                                                         fromRef=dict(id='refs/heads/fb-ASIM-81-network'),
                                                         toRef=dict(id='refs/heads/features'),
                                                         displayId='fb-ASIM-81-network',
                                                         links=make_link('url.com/for/10'))]
    mock_stash_api['PROJ-B']['repo3']['pull_requests'] = [dict(id='17',
                                                         fromRef=dict(id='refs/heads/fb-ASIM-81-network'),
                                                         toRef=dict(id='refs/heads/master'),
                                                         displayId='fb-ASIM-81-network',
                                                         links=make_link('url.com/for/17'))]

    call_merge('ASIM-81', [
        r'PRs in repositories for branch `fb-ASIM-81-network` have different targets:',
        r'`repo1`: \[PR#10\]\(url.com/for/10\) targets `refs/heads/features`',
        r'`repo3`: \[PR#17\]\(url.com/for/17\) targets `refs/heads/master`',
        r'Fix those PRs and try again.',
        r'Alternately you can pass `--force` to force the merge with different targets!'
    ])


def test_branch_commits_without_pr(mock_stash_api):
    from_branch = 'refs/heads/fb-ASIM-81-network'
    to_branch = 'refs/heads/master'
    mock_stash_api['PROJ-B']['repo3']['commits'] = {(from_branch, to_branch): ['A', 'B']}
    mock_stash_api['PROJ-A']['repo1']['commits'] = {(from_branch, to_branch): ['C']}
    pr_link = re.escape(
        'https://myserver.com/stash/projects/PROJ-A/repos/repo1/compare/commits?'
        'sourceBranch=fb-ASIM-81-network&'
        'targetBranch=refs%2Fheads%2Fmaster'
    )
    call_merge('ASIM-81', [
        r'These repositories have commits in `fb-ASIM-81-network` but no PRs:',
        r'`repo1`: \*\*1 commit\*\* \(\[create PR\]\({pr_link}\)\)'.format(pr_link=pr_link),
        r'You need to create PRs for your changes before merging this branch.',
    ])


def test_branch_missing(mock_stash_api):
    mock_stash_api['PROJ-A']['repo1']['branches'] = []
    call_merge('ASIM-81', [
        r'Branch `fb-ASIM-81-network` merged into:',
        r':white_check_mark: `repo3` **2 commits** -> `master`',
        r'Branch deleted from repositories: `repo3`',
    ])


def test_merge_conflicts(mock_stash_api):
    mock_stash_api['PROJ-B']['repo3']['pull_request'] = {
        '10': DummyPullRequest(False),
    }
    call_merge('ASIM-81', [
        r'The PRs below for branch `fb-ASIM-81-network` have conflicts:',
        r'`repo3`: \[PR#10\]\(url.com/for/10\) \*\*CONFLICTS\*\*',
        r'Fix the conflicts and try again. :wink:',
    ])
    assert mock_stash_api['PROJ-B']['repo3']['pull_request']['10'].merged_version is None


def test_merge_success(mock_stash_api):
    pull_request = mock_stash_api['PROJ-B']['repo3']['pull_request']['10']
    call_merge('ASIM-81', [
        r'Branch `fb-ASIM-81-network` merged into:',
        r':white_check_mark: `repo3` **2 commits** -> `master`',
        r'`repo1` - (no changes)',
        r'Branch deleted from repositories: `repo1`, `repo3`'
    ])
    assert pull_request.merged_version == '10'
    assert mock_stash_api['PROJ-A']['repo1']['branches'] == [
        dict(id='refs/heads/fb-SSRL-1890-py3', displayId='fb-SSRL-1890-py3'),
    ]
    assert mock_stash_api['PROJ-A']['repo2']['branches'] == [
        dict(id='refs/heads/fb-SSRL-1890-py3', displayId='fb-SSRL-1890-py3'),
    ]
    assert mock_stash_api['PROJ-B']['repo3']['branches'] == [
        dict(id='refs/heads/fb-SSRL-1890-py3', displayId='fb-SSRL-1890-py3'),
    ]


def test_prs_with_different_targets_force_merge(mock_stash_api):
    mock_stash_api['PROJ-A']['repo1']['commits'] = {
        ('refs/heads/fb-ASIM-81-network', 'refs/heads/features'): ['C', 'D'],
    }
    mock_stash_api['PROJ-A']['repo1']['pull_requests'] = [dict(id='10',
                                                         fromRef=dict(id='refs/heads/fb-ASIM-81-network'),
                                                         toRef=dict(id='refs/heads/features'),
                                                         displayId='fb-ASIM-81-network',
                                                         links=make_link('url.com/for/10'),
                                                         version='10')]
    mock_stash_api['PROJ-A']['repo1']['pull_request'] = {
        '10': DummyPullRequest(True),
    }

    call_merge('ASIM-81', [
        r'Branch `fb-ASIM-81-network` merged into:',
        r':white_check_mark: `repo1` **2 commits** -> `features`',
        r':white_check_mark: `repo3` **2 commits** -> `master`',
        r'Branch deleted from repositories: `repo1`, `repo3`'
    ], force=True)


def test_no_pull_requests(mock_stash_api):
    del mock_stash_api['PROJ-B']['repo3']['pull_requests']
    call_merge('ASIM-81', [
        r'No PRs are open with text `"ASIM-81"`',
    ])


def test_no_matching_branch(mock_stash_api):
    call_merge('FOOBAR-81', [
        r'Could not find any branch with text `"FOOBAR-81"` in any repositories of Stash projects: '
        r'`PROJ-A`, `PROJ-B` nor Github organizations: .',
    ])


pytest_plugins = ["errbot.backends.test"]
extra_plugin_dir = '.'


def test_merge_default_branch(mock_stash_api):
    from_branch = "fb-SSRL-1890-py3"
    mock_stash_api['PROJ-A']['repo1']['pull_requests'] = [dict(id='10',
                                                         fromRef=dict(id="refs/heads/" + from_branch),
                                                         toRef=dict(id='refs/heads/target_branch'),
                                                         displayId=from_branch,
                                                         links=make_link('url.com/for/10'),
                                                         version='10')]
    mock_stash_api['PROJ-A']['repo1']['pull_request'] = {
        '10': DummyPullRequest(True),
    }
    mock_stash_api['PROJ-A']['repo1']['commits'] = {
        ("refs/heads/" + from_branch, 'refs/heads/target_branch'): ['A', 'B'],
    }

    mock_stash_api['PROJ-B']['repo3']['branches'].append(
        dict(id='refs/heads/target_branch', displayId='target_branch'),
    )

    mock_stash_api['PROJ-B']['repo3']['commits'] = {
        ("refs/heads/" + from_branch, 'refs/heads/master'): ['C', 'D'],
    }


    call_merge(from_branch, [
        r'Branch `fb-SSRL-1890-py3` merged into:',
        r':white_check_mark: `repo1` **2 commits** -> `target_branch`',
        r'`repo2` - (no changes)',
        r'`repo3` - (no changes)',
        r'Branch deleted from repositories: `repo1`, `repo2`, `repo3`'
    ])


class TestBot:
    """Tests for the bot commands"""

    @pytest.fixture
    def testbot(self, testbot):
        from errbot.backends.test import TestPerson
        testbot.bot.sender = TestPerson('fry@localhost', nick='fry')
        return testbot

    @pytest.fixture(autouse=True)
    def stash_plugin(self, testbot):
        stash_plugin = testbot.bot.plugin_manager.get_plugin_obj_by_name('Stash')
        stash_plugin.config = {
            'STASH_URL': 'https://my-server.com/stash',
            'STASH_PROJECTS': ['PROJ-A', 'PROJ-B', 'PROJ-FOO'],
        }
        return stash_plugin

    def test_token(self, testbot, stash_plugin, monkeypatch):
        monkeypatch.setattr(stash_plugin, 'config', None)
        testbot.push_message('!stash token')
        response = testbot.pop_message()
        assert 'Stash plugin not configured, contact an admin.' in response

        monkeypatch.undo()
        testbot.push_message('!stash token')
        response = testbot.pop_message()
        assert 'Stash API Token not configured' in response
        assert 'https://my-server.com/stash/plugins/servlet/access-tokens/manage' in response

        testbot.push_message('!stash token secret-token')
        response = testbot.pop_message()
        assert response == 'Token saved.'

        testbot.push_message('!stash token')
        response = testbot.pop_message()
        assert response == 'Your API Token is: secret-token (user: fry)'

        testbot.push_message('!github token github-secret-token')
        response = testbot.pop_message()
        assert response == 'Github token saved.'

        testbot.push_message('!github token')
        response = testbot.pop_message()
        assert response == 'Your Github Token is: github-secret-token (user: fry)'

    def test_merge(self, testbot, mock_stash_api):
        testbot.push_message('!merge ASIM-81')
        response = testbot.pop_message()
        assert 'Stash API Token not configured' in response

        testbot.push_message('!stash token secret-token')
        testbot.pop_message()

        testbot.push_message('!merge ASIM-81')
        response = testbot.pop_message()
        assert response == (
            'Could not find any branch with text "ASIM-81" in any repositories '
            'of Stash projects: PROJ-A, PROJ-B, PROJ-FOO nor Github organizations: .'
        )

    def test_version(self, testbot):
        testbot.push_message('!version')
        response = testbot.pop_message()
        assert '1.0.0' in response


@pytest.fixture
def github_api(mocker):
    api = GithubAPI()
    mocker.patch.object(api, '_github', autospec=True)
    return api


@pytest.fixture
def github_inner_mock(github_api):
    return github_api._github


@pytest.fixture
def github_get_repo(github_inner_mock):
    return github_inner_mock.get_organization.return_value.get_repo


def test_github_fetch_repos(github_api, github_inner_mock):
    repos = ['conda-devenv', 'deps', 'alfasim-sdk']
    github_inner_mock.get_organization.return_value.get_repos.return_value = repos

    assert github_api.fetch_repos("esss") == repos
    github_inner_mock.get_organization.assert_called_with('esss')


def test_github_fetch_branches(github_api, github_get_repo):
    github_get_repo.return_value.get_branch.return_value = 'single-branch'
    github_get_repo.return_value.get_branches.return_value = ['branch-1', 'branch-2', 'branch-3']

    branches = github_api.fetch_branches('esss', 'alfasim-sdk', branch_name='something')
    assert branches == ['single-branch']

    branches = github_api.fetch_branches('esss', 'alfasim-sdk')
    assert branches == ['branch-1', 'branch-2', 'branch-3']


def test_github_delete_branch(github_api, github_get_repo):
    git_repo = GitRepo('repo')
    github_get_repo.return_value = git_repo

    with pytest.raises(AssertionError, match="Trying to delete the wrong branch, check the PR ID."):
        github_api.delete_branch('esss', 'jira2latex', 'fb-a', pr_id=42)

    git_repo.name = 'fb-a'
    github_api.delete_branch('esss', 'jira2latex', 'fb-a', pr_id=42)


def test_github_fetch_pull_requests(github_api, github_inner_mock, github_get_repo):
    github_get_repo.return_value.get_pulls.return_value = [1, 2, 3]

    prs = github_api.fetch_pull_requests('esss', 'jira2latex')
    github_inner_mock.get_organization.assert_called_with('esss')
    github_get_repo.assert_called_with('jira2latex')
    assert prs == [1, 2, 3]


def test_github_fetch_pull_request(github_api, github_inner_mock, github_get_repo):
    github_get_repo.return_value.get_pull.return_value = 'dummy pr'

    pr = github_api.fetch_pull_request('esss', 'jira2latex', 42)
    github_inner_mock.get_organization.assert_called_with('esss')
    github_get_repo.assert_called_with('jira2latex')
    assert pr == 'dummy pr'


def test_github_fetch_repo_commits(github_api, github_inner_mock, github_get_repo):
    github_get_repo.return_value.compare.return_value.commits = [1, 2, 3]

    commits = github_api.fetch_repo_commits('esss', 'jira2latex', 'develop', 'master')
    assert commits == [1, 2, 3]
    github_inner_mock.get_organization.assert_called_with('esss')
    github_get_repo.assert_called_with('jira2latex')
    github_get_repo.return_value.compare.assert_called_with('master', 'develop')


def test_github_create_plans(github_api, mock_stash_api, github_inner_mock, github_get_repo):
    github_get_repo.return_value.get_branches.return_value = ['branch-1', 'branch-2']
    github_inner_mock.get_organization.return_value.get_repos.return_value = [
        GitRepo('repo-1'), GitRepo('repo-2')]

    github_get_repo.return_value.get_pulls.return_value = [GitPR(1), GitPR(2)]

    plans = create_plans(mock_stash_api, github_api, [], ['esss'], 'fb-branch')
    github_inner_mock.get_organization.assert_called_with('esss')

    assert len(plans) == 2
    assert [plan.slug for plan in plans] == ['repo-1', 'repo-2']
    assert [plan.to_branch for plan in plans] == ['master-branch', 'master-branch']
    assert [len(plan.pull_requests) for plan in plans] == [2, 2]


class GitRepo:
    def __init__(self, name):
        self.owner = lambda: None
        self.owner.name = 'esss'
        self.name = name

    def get_git_ref(self, ref):
        result = lambda: None
        result.ref = "heads/{name}".format(name=self.name)
        result.delete = lambda: None
        return result

    def get_pull(self, pr_id):
        return GitPR(pr_id)

class GitPR:
    def __init__(self, id):
        self.id = id
        self.head = lambda : None
        self.head.ref = 'fb-branch'

        self.base = lambda: None
        self.base.ref = 'master-branch'