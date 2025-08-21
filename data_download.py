from datamule import Portfolio

portfolio = Portfolio('8k_2024')
portfolio.download_submissions(submission_type='8-K',filing_date=('2024-01-01','2024-12-31'))