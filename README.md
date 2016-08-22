# funda-scraper
Scraper of the Dutch real estate website [Funda.nl](http://www.funda.nl/), written in Python Scrapy.

## Basic usage
There are two spiders: 

1. `funda_spider` scrapes all properties for sale in a certain city, such as [http://www.funda.nl/koop/amsterdam/](http://www.funda.nl/koop/amsterdam/),
2. `funda_spider_sold` scrapes data on properties which have recently been sold, such as those listed on [http://www.funda.nl/koop/verkocht/amsterdam/](http://www.funda.nl/koop/verkocht/amsterdam/).

After installing [Scrapy](www.scrapy.org), in the project directory simply run the command

`scrapy crawl funda_spider -a place=amsterdam -o amsterdam_for_sale.json`

to generate a JSON file `amsterdam_for_sale.json` with all houses for sale listed on [http://www.funda.nl/koop/amsterdam/](http://www.funda.nl/koop/amsterdam/) and its subpages. The keyword argument `place` can be used to scrape data from other cities; for example `place=rotterdam` will scrape data from [http://www.funda.nl/koop/rotterdam/](http://www.funda.nl/koop/rotterdam/).

For recently sold homes, run

`scrapy crawl funda_spider_sold -a place=amsterdam -o amsterdam_sold.json`

to generate an `amsterdam_sold.json` with data from [http://www.funda.nl/koop/verkocht/amsterdam/](http://www.funda.nl/koop/verkocht/amsterdam/). Alternatively, CSV output can be generated by typing `amsterdam_sold.csv` extension instead of `amsterdam_sold.json`.

