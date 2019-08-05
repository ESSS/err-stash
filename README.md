# err-stash

[![Travis branch](https://img.shields.io/travis/ESSS/err-stash/master.svg)](https://travis-ci.org/ESSS/err-stash/)

[errbot plugin](http://errbot.io/en/latest/) to interact with Stash.

# Usage

Talk with the bot for help:

```
!help Stash
```

# Development

Clone:

```
git clone git@github.com:ESSS/err-stash.git
cd err-stash
```

Create a **pure** virtual environment with Python 3.6 and activate it. Using `conda`:

```
conda create -n py36 python=3.6
W:\Miniconda\envs\py36\python.exe -m venv .env36
.env36\Scripts\activate
```

**It is important to use a pure virtual environment and not a conda environment** otherwise
`pip install` might break `conda`.

Install dependencies:

```
pip install -r dev-requirements.txt
```

Run tests:

```
pytest tests.py
```

## Run bot in text mode

Create a bot for local development:

```
errbot --init
```

And edit the generated `config.py`:

* Change `@CHANGE_ME` to your username.
* Change `BOT_EXTRA_PLUGIN_DIR` to point to the current directory.

Start it up with:

```
errbot -T
```

Execute to configure the bot:

```
!plugin config Stash {
'STASH_URL': 'https://eden.esss.com.br/stash',
'STASH_PROJECTS': ['ESSS'],
'GITHUB_ORGANIZATIONS': ['ESSS'],
}
```

Copy and paste this configuration, probably the default is enough for you.
