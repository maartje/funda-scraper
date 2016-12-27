## Overview
Scraper of the Dutch real estate website [Funda.nl](http://www.funda.nl/), written in Python using [Scrapy](https://scrapy.org/). Based on [funda-scraper](https://github.com/jackha/funda-scraper).


## Basic Usage
There are two spiders: `funda_spider` scrapes data on houses for sale in a certain city, such as those listed on [http://www.funda.nl/koop/amsterdam/](http://www.funda.nl/koop/amsterdam/), `funda_spider_sold` scrapes data on houses which have recently been sold, such as those listed on [http://www.funda.nl/koop/verkocht/amsterdam/](http://www.funda.nl/koop/verkocht/amsterdam/).
The spiders can be run with the following commands:

`scrapy crawl funda_spider -a place=amsterdam -o amsterdam_for_sale.csv -s LOG_LEVEL=ERROR`

`scrapy crawl funda_spider_sold -a place=amsterdam -o amsterdam_sold.csv -s LOG_LEVEL=ERROR`

The keyword 'place' specifies the city for which the data is scraped. The output format can be set alternatively to .json by typing 'amsterdam_sold.json' instead of 'amsterdam_sold.csv'.

## Installation

Install [Scrapy](https://scrapy.org/) in the project directory

- sudo apt-get install python-pip python-scrapy

## Motivation

The data is collected as part of a data science study project.