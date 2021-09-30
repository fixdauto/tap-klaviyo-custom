# tap-klaviyo-custom

`tap-klaviyo-custom` is a Singer tap that produces JSON-formatted data for lists and list members endpoints from the Klaviyo API.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

```bash
pipx install tap-klaviyo-custom
```

## Configuration

### Accepted Config Options

```bash
{
  "api_key": "pk_XYZ",
  "start_date": "2017-01-01T00:00:00Z",
  "user_agent": "email_address",
  "listIDs": ["List of list IDs"]
}
```

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-klaviyo-custom --about
```

## Usage

You can easily run `tap-klaviyo-custom` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-klaviyo-custom --version
tap-klaviyo-custom --help
tap-klaviyo-custom --config CONFIG --discover > ./catalog.json
```

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_klaviyo_custom/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-klaviyo-custom` CLI interface directly using `poetry run`:

```bash
poetry run tap-klaviyo-custom --help
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
