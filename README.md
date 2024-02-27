# NYC Taxi Data ETL

This module contains functions for extracting, transforming, and loading NYC taxi data from the [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) using the [`dlt`](https://dlthub.com/) library in Python.

## Introduction

The code provides a set of functions to retrieve, process, and store data from various NYC taxi trip datasets. It includes functions for extracting data, normalizing the schema, transforming it into a standardized format, and loading it into a data warehouse for further analysis. 

## Features

- Extracts and transforms yellow, green, and fhvhv taxi trip data
- Supports retrieving data from remote sources via HTTP requests
- Provides logging functionality to track data extraction and loading progress

### Performace Analytics

Separate Runs:

    - Green Taxi: ~03 mins [2019-2023]
    - Yellow Taxi: ~48 mins [2019-2023]

## Installation

Clone this repository to your local machine and install the required dependencies using pip:

```bash
git clone https://github.com/snehangsude/nyc_taxi.git
cd nyc_taxi
pip install -r requirements.txt
```

## Usage

The module can be used to extract and load NYC taxi trip data into a data warehouse. Here's how to use it:

- Configure the module settings in `.dlt/config.toml`.
- Run the main script `nyc_taxi.py` to start the ETL process.
- Monitor the logs in `.logs/nyc_taxi.log` to track the progress and status of the ETL process.

## Contributing

Contributions to this project are welcome! Please follow these guidelines when contributing:

- Fork the repository
- Create a new branch for your feature or bug fix
- Make your changes and test them thoroughly
- Submit a pull request with a clear description of your changes (should include message)

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/snehangsude/nyc_taxi_pipelines?tab=MIT-1-ov-file) file for details.

## Contact

For questions or feedback, please contact the author:

Snehangsu De
Email: desnehangsu@gmail.com
GitHub: [snehangsude](https://github.com/snehangsude)

