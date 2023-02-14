import yfinance


def ticker_value_for_interval(ticker, start_date, end_date):
	path = f"./source_stocks/{ticker}.csv"
	yfinance.Ticker(ticker).history(
			period="1d",
			interval="1h",
			start=start_date,
			end=end_date,
			prepost=True
	).to_csv(path)


ticker_value_for_interval("AAPL", "2023-01-08", "2023-01-15")
ticker_value_for_interval("GOOG", "2023-01-08", "2023-01-15")