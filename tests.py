import re
from collections import OrderedDict

import pytest
import stashy
import stashy.errors

from err_stash import merge, StashAPI, CheckError


class DummyPullRequest:

    def __init__(self, can_merge):
        self._can_merge = can_merge
        self.merged_version = None

    def can_merge(self):
        return self._can_merge

    def merge(self, version):
        self.merged_version = version


@pytest.fixture
def mock_api(mocker):
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
        lines = list(merge('https://myserver.com/stash', ['PROJ-A', 'PROJ-B'], username='fry', password='PASSWORD123',
                           branch_text=branch_text, confirm=True, force=force))
    except CheckError as e:
        lines = e.lines
    from _pytest.pytester import LineMatcher
    matcher = LineMatcher(list(lines))
    matcher.re_match_lines(matching_lines)


def make_link(url):
    return dict(self=[dict(href=url)])


def test_duplicate_branches(mock_api):
    mock_api['PROJ-A']['repo1']['branches'] = [dict(id='refs/heads/fb-ASIM-81-network', displayId='fb-ASIM-81-network'),
                                               dict(id='refs/heads/fb-ASIM-81-network-test',
                                                    displayId='fb-ASIM-81-network-test')]
    mock_api['PROJ-A']['repo2']['branches'] = [dict(id='refs/heads/fb-ASIM-81-network')]
    mock_api['PROJ-B']['repo3']['branches'] = [dict(id='refs/heads/fb-ASIM-81-network'),
                                               dict(id='refs/heads/fb-SSRL-1890-py3')]

    call_merge('ASIM-81', [
        r'More than one branch matches the text `"ASIM-81"`:',
        r'`repo1`: `fb-ASIM-81-network`, `fb-ASIM-81-network-test`',
        r'Use a more complete text.*',
    ])


def test_multiples_prs_for_same_branch(mock_api):
    mock_api['PROJ-A']['repo1']['pull_requests'] = [dict(id='10',
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


def test_prs_with_different_targets(mock_api):
    mock_api['PROJ-A']['repo1']['pull_requests'] = [dict(id='10',
                                                         fromRef=dict(id='refs/heads/fb-ASIM-81-network'),
                                                         toRef=dict(id='refs/heads/features'),
                                                         displayId='fb-ASIM-81-network',
                                                         links=make_link('url.com/for/10'))]
    mock_api['PROJ-B']['repo3']['pull_requests'] = [dict(id='17',
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


def test_branch_commits_without_pr(mock_api):
    from_branch = 'refs/heads/fb-ASIM-81-network'
    to_branch = 'refs/heads/master'
    mock_api['PROJ-B']['repo3']['commits'] = {(from_branch, to_branch): ['A', 'B']}
    mock_api['PROJ-A']['repo1']['commits'] = {(from_branch, to_branch): ['C']}
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


def test_branch_missing(mock_api):
    mock_api['PROJ-A']['repo1']['branches'] = []
    call_merge('ASIM-81', [
        r'Branch `fb-ASIM-81-network` merged into:',
        r':white_check_mark: `repo3` **2 commits** -> `master`',
        r'Branch deleted from repositories: `repo3`',
    ])


def test_merge_conflicts(mock_api):
    mock_api['PROJ-B']['repo3']['pull_request'] = {
        '10': DummyPullRequest(False),
    }
    call_merge('ASIM-81', [
        r'The PRs below for branch `fb-ASIM-81-network` have conflicts:',
        r'`repo3`: \[PR#10\]\(url.com/for/10\) \*\*CONFLICTS\*\*',
        r'Fix the conflicts and try again. :wink:',
    ])
    assert mock_api['PROJ-B']['repo3']['pull_request']['10'].merged_version is None


def test_merge_success(mock_api):
    pull_request = mock_api['PROJ-B']['repo3']['pull_request']['10']
    call_merge('ASIM-81', [
        r'Branch `fb-ASIM-81-network` merged into:',
        r':white_check_mark: `repo3` **2 commits** -> `master`',
        r'`repo1` - (no changes)',
        r'Branch deleted from repositories: `repo1`, `repo3`'
    ])
    assert pull_request.merged_version == '10'
    assert mock_api['PROJ-A']['repo1']['branches'] == [
        dict(id='refs/heads/fb-SSRL-1890-py3', displayId='fb-SSRL-1890-py3'),
    ]
    assert mock_api['PROJ-A']['repo2']['branches'] == [
        dict(id='refs/heads/fb-SSRL-1890-py3', displayId='fb-SSRL-1890-py3'),
    ]
    assert mock_api['PROJ-B']['repo3']['branches'] == [
        dict(id='refs/heads/fb-SSRL-1890-py3', displayId='fb-SSRL-1890-py3'),
    ]


def test_prs_with_different_targets_force_merge(mock_api):
    mock_api['PROJ-A']['repo1']['commits'] = {
        ('refs/heads/fb-ASIM-81-network', 'refs/heads/features'): ['C', 'D'],
    }
    mock_api['PROJ-A']['repo1']['pull_requests'] = [dict(id='10',
                                                         fromRef=dict(id='refs/heads/fb-ASIM-81-network'),
                                                         toRef=dict(id='refs/heads/features'),
                                                         displayId='fb-ASIM-81-network',
                                                         links=make_link('url.com/for/10'),
                                                         version='10')]
    mock_api['PROJ-A']['repo1']['pull_request'] = {
        '10': DummyPullRequest(True),
    }

    call_merge('ASIM-81', [
        r'Branch `fb-ASIM-81-network` merged into:',
        r':white_check_mark: `repo1` **2 commits** -> `features`',
        r':white_check_mark: `repo3` **2 commits** -> `master`',
        r'Branch deleted from repositories: `repo1`, `repo3`'
    ], force=True)


def test_no_pull_requests(mock_api):
    del mock_api['PROJ-B']['repo3']['pull_requests']
    call_merge('ASIM-81', [
        r'No PRs are open with text `"ASIM-81"`',
    ])


def test_no_matching_branch(mock_api):
    call_merge('FOOBAR-81', [
        r'Could not find any branch with text `"FOOBAR-81"` in any repositories of projects `PROJ-A`, `PROJ-B`.',
    ])


pytest_plugins = ["errbot.backends.test"]
extra_plugin_dir = '.'


def test_merge_default_branch(mock_api):
    from_branch = "fb-SSRL-1890-py3"
    mock_api['PROJ-A']['repo1']['pull_requests'] = [dict(id='10',
                                                         fromRef=dict(id="refs/heads/" + from_branch),
                                                         toRef=dict(id='refs/heads/target_branch'),
                                                         displayId=from_branch,
                                                         links=make_link('url.com/for/10'),
                                                         version='10')]
    mock_api['PROJ-A']['repo1']['pull_request'] = {
        '10': DummyPullRequest(True),
    }
    mock_api['PROJ-A']['repo1']['commits'] = {
        ("refs/heads/" + from_branch, 'refs/heads/target_branch'): ['A', 'B'],
    }

    mock_api['PROJ-B']['repo3']['branches'].append(
        dict(id='refs/heads/target_branch', displayId='target_branch'),
    )

    mock_api['PROJ-B']['repo3']['commits'] = {
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
        assert response == 'You API Token is: secret-token (user: fry)'

    def test_merge(self, testbot, mock_api):
        testbot.push_message('!merge ASIM-81')
        response = testbot.pop_message()
        assert 'Stash API Token not configured' in response

        testbot.push_message('!stash token secret-token')
        testbot.pop_message()

        testbot.push_message('!merge ASIM-81')
        response = testbot.pop_message()
        assert response == (
            'Could not find any branch with text "ASIM-81" in any repositories '
            'of projects PROJ-A, PROJ-B, PROJ-FOO.'
        )

    def test_version(self, testbot):
        testbot.push_message('!version')
        response = testbot.pop_message()
        assert '1.0.0' in response


