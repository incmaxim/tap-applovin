# tap-applovin

`tap-applovin` is a Singer tap for applovin.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Features
- Leverages the `report` endpoint from the [Applovin API]() in order to grab hourly summaries
of ad performance by campaign, creative, country, and platform.
- Configure the date range you want to grab data from using the `report_range_days` config variable.
- Built with the Singer SDK for extensibility and Meltano compatibility.

## Requirements

- Python 3.9+

- Applovin API key

- Singer SDK

- Meltano (optional, for pipeline orchestration)

## Installation

1. Clone the repository:
  ```bash
  git clone https://github.com/yourusername/tap-applovin.git
  cd tap-applovin
  ```
2. Install the dependencies:
  ```bash
  poetry install
  ```
3. Activate the Poetry virtual environment:
  ```bash
  poetry shell
  ```

## Configuration

1. Create a config.json file with the following structure:
  ```bash
  {
    "api_key": "YOUR_APPOVIN_API_KEY",
    "report_range_days": 30
  }
  ```
  - `api_key`: Your Applovin API key.

  - `report_range_days`: Number of days of data to fetch (e.g., 30 for the last 30 days).

2. Place config.json in the root directory or pass its path as an argument when running the tap.

## Running the Tap

1. Run the tap standalone:
  ```bash
  poetry run python -m tap_applovin --config config.json
  ```
2. Use with Meltano for pipeline orchestration:
  - Add the tap to your Meltano project:
  ```bash
  meltano add extractor tap-applovin
  ```
  - Configure the tap in meltano.yml:
  ```bash
  extractors:
    tap-applovin:
      config:
        api_key: YOUR_APPOVIN_API_KEY
        report_range_days: 30
  ```
  - Run the tap via Meltano:
  ```bash
  meltano run tap-applovin target-your-target
  ```

### Accepted Config Options

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-applovin --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Testing with [Meltano](https://www.meltano.com)

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-applovin
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-applovin --version
# OR run a test `elt` pipeline:
meltano run tap-applovin target-jsonl
```

## Customization

The streams.py file provides the main logic for fetching and processing data. Key points
of customization include:
- **Columns:** Update the columns list to fetch additional fields.

- **Date Range:** Modify the date_range method to adjust date partitioning.

- **API Parameters:** Customize the get_url_params method to include additional query parameters.

## Development

1. Install development dependencies using Poetry:
  ```bash
  poetry install --with dev
  ```
2. Run tests:
  ```bash
  poery run pytest
  ```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
