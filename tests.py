import re
from collections import OrderedDict

import pytest
import stashy
import stashy.errors
from github import GithubException

from err_stash import GithubAPI, merge, StashAPI, CheckError, create_plans, ensure_text_matches_unique_branch


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
        r'The PRs below for branch `fb-ASIM-81-network` have problems such as conflicts, build requirements, etc:',
        r'`repo3`: \[PR#10\]\(url.com/for/10\)',
        r'Fix them and try again.',
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
            'GITHUB_ORGANIZATIONS': ['GIT-FOO']
        }
        return stash_plugin

    @pytest.mark.parametrize(
        'stash_projects, github_organizations, expected_response',
        [
            ([], [], 'STASH_PROJECTS not configured. Use !plugin config Stash to configure it.'),
            (['P'], [], 'GITHUB_ORGANIZATIONS not configured. Use !plugin config Stash to configure it.'),
            # TODO: for now we can't test the merge with the correct configuration because we can't mock the
            # `GithubAPI` internally on the bot, as it runs on another thread, causing `Bad credentials` error
            # (['P'], ['Z'], '....'),
        ],
    )
    def test_merge(
            self,
            testbot,
            stash_plugin,
            monkeypatch,
            stash_projects,
            github_organizations,
            expected_response
    ):
        testbot.push_message('!merge ASIM-81')
        response = testbot.pop_message()
        assert 'Stash API Token not configured' in response

        config = {
            'STASH_URL': 'https://my-server.com/stash',
            'STASH_PROJECTS': stash_projects,
            'GITHUB_ORGANIZATIONS': github_organizations
        }
        monkeypatch.setattr(stash_plugin, 'config', config)

        testbot.push_message('!stash token secret-token')
        response = testbot.pop_message()
        assert response == 'Token saved.'

        testbot.push_message('!github token github-secret-token')
        response = testbot.pop_message()
        assert response == 'Github token saved.'

        testbot.push_message('!merge ASIM-81')
        response = testbot.pop_message()

        assert response == expected_response

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

    def test_version(self, testbot):
        testbot.push_message('!version')
        response = testbot.pop_message()
        assert '1.0.0' in response


@pytest.fixture
def github_api(mocker):
    api = GithubAPI()
    mocker.patch.object(api, '_github', autospec=True)
    organizations = {'esss' : 'esss'}
    repos = {
        'esss': {
            'conda-devenv': GitRepo('conda-devenv'),
            'deps': GitRepo('deps'),
            'jira2latex': GitRepo('jira2latex')
        },
    }
    api.organizations = organizations
    api.repos = repos
    return api


@pytest.fixture
def github_inner_mock(github_api):
    return github_api._github


@pytest.fixture
def github_get_repo(github_inner_mock):
    return github_inner_mock.get_organization.return_value.get_repo


def test_github_fetch_repos(github_api):
    repos = github_api.fetch_repos("esss")
    assert {repo.name for repo in repos} == {'conda-devenv', 'deps', 'jira2latex'}

@pytest.mark.parametrize('branch_name, expected_branches', [
    ('', ['master', 'branch-1', 'branch-2', 'branch-3']),
    ('non-existing', []),
])
def test_github_fetch_branches(github_api, branch_name, expected_branches):
    branches = github_api.fetch_branches('esss', 'jira2latex', branch_name=branch_name)
    assert [branch.name for branch in branches] == expected_branches


def test_github_delete_branch(github_api):
    github_api.delete_branch('esss', 'jira2latex', 'branch-1')


def test_github_fetch_pull_requests(github_api):
    prs = github_api.fetch_pull_requests('esss', 'jira2latex')
    assert [pr.id for pr in prs] == [0, 1, 2]


def test_github_fetch_pull_request(github_api):
    pr = github_api.fetch_pull_request('esss', 'jira2latex', 42)
    assert pr.id == 42


def test_github_fetch_repo_commits(github_api):
    commits = github_api.fetch_repo_commits('esss', 'jira2latex', 'develop', 'master')
    assert commits == [0, 1, 2]


def test_github_create_plans(github_api, mock_stash_api):
    plans = create_plans(mock_stash_api, github_api, [], ['esss'], 'branch-1')
    ensure_text_matches_unique_branch(plans, 'branch-1')

    assert len(plans) == 3
    for plan in plans:
        # need to assert this way because the order is not a guaranteed
        assert plan.slug in ['conda-devenv', 'deps', 'jira2latex']

    assert [plan.to_branch for plan in plans] == ['refs/heads/master', 'refs/heads/master', 'refs/heads/master']
    assert [len(plan.pull_requests) for plan in plans] == [1, 1, 1]


class GitRepo:

    def __init__(self, name):
        self.owner = lambda: None
        self.owner.name = 'esss'
        self.name = name
        self.branches = [
            GitBranch('master'),
            GitBranch('branch-1'),
            GitBranch('branch-2'),
            GitBranch('branch-3')
        ]

    def get_git_ref(self, ref):
        result = lambda: None
        result.ref = "heads/{name}".format(name=ref)
        result.delete = lambda: None
        return result

    def get_pull(self, pr_id):
        return GitPR(pr_id, 'branch-1', 'master')

    def get_pulls(self):
        return [
            GitPR(0, 'branch-1', 'master'),
            GitPR(1, 'branch-2', 'branch-3'),
            GitPR(2, 'branch-3', 'master')
        ]

    def compare(self, to_branch, from_branch):
        result = lambda: None
        result.commits = [0, 1, 2]
        return result

    def get_branches(self):
        return self.branches

    def get_branch(self, branch_name):
        for branch in self.branches:
            if branch_name == branch.name:
                return branch

        raise GithubException(404, "not found")

class GitPR:

    def __init__(self, id, from_branch, to_branch):
        self.id = id
        self.head = lambda : None
        self.head.ref = from_branch

        self.base = lambda: None
        self.base.ref = to_branch

class GitBranch:

    def __init__(self, name):
        self.name = name
        self.items = dict()
        self.items['displayId'] = name

    def __getitem__(self, value):
        return self.items.get(value, '')