SELECT
	id,
	AllContributions.timestamp,
	type,
	text,
	total_pos_vader_sentiment,
	-1 * total_neg_vader_sentiment AS total_neg_vader_sentiment,
	total_compound_vader_sentiment,
	total_pos_finbert_sentiment,
	-1 * total_neg_finbert_sentiment AS total_neg_finbert_sentiment,
	total_compound_finbert_sentiment,
	upvotes,
	subreddit_id,
	[^GSPC_closing_price] AS SP500,
	SCHW_closing_price AS SCHW,
	submission_id,
	comment_id,
	author_id,
	CASE
		WHEN (total_pos_vader_sentiment > total_neg_vader_sentiment) THEN 'Positive'
		WHEN (total_pos_vader_sentiment < total_neg_vader_sentiment) THEN 'Negative'
		ELSE 'Neutral'
	END AS indicator,
	CASE 
		WHEN LOWER(text) LIKE '%schw%' THEN 'Charles Schwab'
		WHEN LOWER(text) LIKE '%fidelity%' THEN 'Fidelity'
		WHEN 
			LOWER(text) LIKE '%robinhood%' OR
			LOWER(text) LIKE '%robin hood%' OR
			LOWER(text) LIKE '%hood%'
		THEN 'Robinhood'
		WHEN 
			LOWER(text) LIKE '%blackrock%' OR
			LOWER(text) LIKE '%black rock%'
		THEN 'BlackRock'
		WHEN 
			LOWER(text) LIKE '%chase%' OR
			LOWER(text) LIKE '%jp morgan%' OR
			LOWER(text) LIKE '%jpm%'
		THEN 'JP Morgan Chase'
		WHEN LOWER(text) LIKE '%etoro%' then 'eToro'
		WHEN 
			LOWER(text) LIKE '%goldman%' OR 
			LOWER(text) LIKE '%sachs%' 
		THEN 'Goldman Sachs'
		WHEN LOWER(text) LIKE '%interactive brokers%' THEN 'Interactive Brokers'
		WHEN LOWER(text) LIKE '%morgan stanley%' THEN 'Morgan Stanley'
		ELSE NULL 
	END AS brand,
	CASE 
		WHEN 
			LOWER(text) LIKE '%SCHD%' OR
			LOWER(text) LIKE '%Schwab U.S. Dividend%' OR
			LOWER(text) LIKE '%Schwab US Dividend%' OR
			LOWER(text) LIKE '%Schwab Dividend%'
		THEN 'SCHD'
		WHEN
			LOWER(text) LIKE '%thinkorswim%' OR
			LOWER(text) LIKE '%TOS%' OR
			LOWER(text) LIKE '%think or swim%'
		THEN 'thinkorswim'
		WHEN 
			LOWER(text) LIKE '%schwab robo-advisor%' OR
			LOWER(text) LIKE '%schwab robo%' OR
			LOWER(text) LIKE '%schwab robo advisor%' OR
			LOWER(text) LIKE '%schwab intelligent portfolio%'
		THEN 'Schwab Robo-Advisor'
		WHEN
			LOWER(text) LIKE '%schwab mobile%' OR
			LOWER(text) LIKE '%schwab app%' OR
			LOWER(text) LIKE '%schwab mobile app%'
		THEN 'Schwab Mobile App'
		WHEN 
			LOWER(text) LIKE '%SWISX%' OR
			LOWER(text) LIKE '%schwab international index%' 
		THEN 'Schwab International Index Fund'
		WHEN
			LOWER(text) LIKE '%FNDX%' OR
			LOWER(text) LIKE '%schwab large company ETF%'
		THEN 'Schwab Fundamental U.S. Large Company Index ETF'
		WHEN
			LOWER(text) LIKE '%SWTSX%' OR
			LOWER(text) LIKE '%schwab total stock market%' 
		THEN 'Schwab Total Stock Market Index Fund'
		WHEN 
			LOWER(text) LIKE '%SWPPX%' OR
			LOWER(text) LIKE '%Schwab S&P 500%'
		THEN 'Schwab S&P 500 Index Fund'
		WHEN
			LOWER(text) LIKE '%SWVXX%' OR
			LOWER(text) LIKE '%schwab money fund%' OR
			LOWER(text) LIke '%schwab value advantage%'
		THEN 'Schwab Value Advantage Money Fund'
	END AS product,
	CASE 
		WHEN 
			LOWER(text) LIKE '%etf%' 
		THEN 'ETFs'
		WHEN
			LOWER(text) LIKE '%crypto%' OR
			LOWER(text) LIKE '%bitcoin%' OR
			LOWER(text) LIKE '%btc%' OR
			LOWER(text) LIKE '%eth%' OR
			LOWER(text) LIKE '%blockchain%' OR
			LOWER(text) LIKE '%token%' OR
			LOWER(text) LIKE '%altcoin%' OR
			LOWER(text) LIKE '%defi%' OR
			LOWER(text) LIKE '%decentralized%' OR
			LOWER(text) LIKE '%smart contract%' OR
			LOWER(text) LIKE '%stablecoin%' 
		THEN 'Cryptocurrency'
		WHEN
			LOWER(text) LIKE '%bond%' 
		THEN 'Bonds'
		WHEN
			LOWER(text) LIKE '%dividend%'
		THEN 'Dividends'
		WHEN 
			LOWER(text) LIKE '%commodit%' OR
			LOWER(text) LIKE '%oil%' OR
			LOWER(text) LIKE '%gold%' OR
			LOWER(text) LIKE '%silver%' OR
			LOWER(text) LIKE '%steel%'
		THEN 'Commodities'
		WHEN 
			LOWER(text) LIKE '%forex%' OR
			LOWER(text) LIKE '%foreign%' OR
			LOWER(text) LIKE '%currency%' OR
			LOWER(text) LIKE '%eur%' OR
			LOWER(text) LIKE '%yen%' OR
			LOWER(text) LIKE '%GBP%' OR
			LOWER(text) LIKE '%pound%' OR
			LOWER(text) LIKE '%AUD%' OR
			LOWER(text) LIKE '%foreign exchange rate%' OR
			LOWER(text) LIKE '%international%'
		THEN 'Foreign Currency'
		WHEN
			LOWER(text) LIKE '%GME%' OR
			LOWER(text) LIKE '%Gamestop%' OR
			LOWER(text) LIKE '%memestock%' OR
			LOWER(text) LIKE '%stonk%' OR
			LOWER(text) LIKE '%hodl%' OR
			LOWER(text) LIKE '%AMC%' OR
			LOWER(text) LIKE '%YOLO%' OR
			LOWER(text) LIKE '%diamond hands%' OR
			LOWER(text) LIKE '%to the moon%' OR
			LOWER(text) LIKE '%Nokia%' OR
			LOWER(text) LIKE '%BlackBerry%' OR
			LOWER(text) LIKE '%tendies%'
		THEN 'Memestocks'
		WHEN
			LOWER(text) LIKE '%nvidia%' OR
			LOWER(text) LIKE '%nvda%' OR
			LOWER(text) LIKE '%advanced micro devices%' OR
			LOWER(text) LIKE '%amd%' OR
			LOWER(text) LIKE '%fortinet%' OR
			LOWER(text) LIKE '%ftnt%' OR
			LOWER(text) LIKE '%apple%' OR
			LOWER(text) LIKE '%aapl%' OR
			LOWER(text) LIKE '%ubiquity%' 
		THEN 'Technology'
		WHEN
			LOWER(text) LIKE '%johnson & johnson%' OR
			LOWER(text) LIKE '%jnj%' OR
			LOWER(text) LIKE '%pfizer%' OR
			LOWER(text) LIKE '%pfe%' OR
			LOWER(text) LIKE '%unitedhealth group%' OR
			LOWER(text) LIKE '%unh%' OR
			LOWER(text) LIKE '%abbott laboratories%' OR
			LOWER(text) LIKE '%abt%' OR
			LOWER(text) LIKE '%merck%' OR
			LOWER(text) LIKE '%mrk%' 
		THEN 'Healthcare'
		WHEN
			LOWER(text) LIKE '%jpmorgan chase%' OR
			LOWER(text) LIKE '%jpm%' OR
			LOWER(text) LIKE '%goldman sachs%' OR
			LOWER(text) LIKE '%gs%' OR
			LOWER(text) LIKE '%visa%' OR
			LOWER(text) LIKE '%v%' OR
			LOWER(text) LIKE '%bank of america%' OR
			LOWER(text) LIKE '%bac%' OR
			LOWER(text) LIKE '%wells fargo%' OR
			LOWER(text) LIKE '%wfc%' 
		THEN 'Financials'
		WHEN
			LOWER(text) LIKE '%amazon%' OR
			LOWER(text) LIKE '%amzn%' OR
			LOWER(text) LIKE '%tesla%' OR
			LOWER(text) LIKE '%tsla%' OR
			LOWER(text) LIKE '%home depot%' OR
			LOWER(text) LIKE '%hd%' OR
			LOWER(text) LIKE '%mcdonald%' OR
			LOWER(text) LIKE '%mcd%' OR
			LOWER(text) LIKE '%nike%' OR
			LOWER(text) LIKE '%nke%' 
		THEN 'Consumer Discretionary'
		WHEN
			LOWER(text) LIKE '%procter & gamble%' OR
			LOWER(text) LIKE '%pg%' OR
			LOWER(text) LIKE '%coca-cola%' OR
			LOWER(text) LIKE '%ko%' OR
			LOWER(text) LIKE '%pepsico%' OR
			LOWER(text) LIKE '%pep%' OR
			LOWER(text) LIKE '%costco%' OR
			LOWER(text) LIKE '%cost%' OR
			LOWER(text) LIKE '%walmart%' OR
			LOWER(text) LIKE '%wmt%' 
		THEN 'Consumer Staples'
		WHEN
			LOWER(text) LIKE '%exxonmobil%' OR
			LOWER(text) LIKE '%xom%' OR
			LOWER(text) LIKE '%chevron%' OR
			LOWER(text) LIKE '%cvx%' OR
			LOWER(text) LIKE '%schlumberger%' OR
			LOWER(text) LIKE '%slb%' OR
			LOWER(text) LIKE '%conocophillips%' OR
			LOWER(text) LIKE '%cop%' OR
			LOWER(text) LIKE '%phillips 66%' OR
			LOWER(text) LIKE '%psx%' 
		THEN 'Energy'
		WHEN
			LOWER(text) LIKE '%honeywell%' OR
			LOWER(text) LIKE '%hon%' OR
			LOWER(text) LIKE '%general electric%' OR
			LOWER(text) LIKE '%ge%' OR
			LOWER(text) LIKE '%3m%' OR
			LOWER(text) LIKE '%mmm%' OR
			LOWER(text) LIKE '%boeing%' OR
			LOWER(text) LIKE '%ba%' OR
			LOWER(text) LIKE '%lockheed martin%' OR
			LOWER(text) LIKE '%lmt%' 
		THEN 'Industrials'
		WHEN
			LOWER(text) LIKE '%dow inc%' OR
			LOWER(text) LIKE '%dow%' OR
			LOWER(text) LIKE '%dupont%' OR
			LOWER(text) LIKE '%dd%' OR
			LOWER(text) LIKE '%newmont corporation%' OR
			LOWER(text) LIKE '%nem%' OR
			LOWER(text) LIKE '%cf industries%' OR
			LOWER(text) LIKE '%cf%' OR
			LOWER(text) LIKE '%eastman chemical%' OR
			LOWER(text) LIKE '%emn%' 
		THEN 'Materials'
		WHEN
			LOWER(text) LIKE '%prologis%' OR
			LOWER(text) LIKE '%pld%' OR
			LOWER(text) LIKE '%simon property group%' OR
			LOWER(text) LIKE '%spg%' OR
			LOWER(text) LIKE '%digital realty trust%' OR
			LOWER(text) LIKE '%dlr%' OR
			LOWER(text) LIKE '%public storage%' OR
			LOWER(text) LIKE '%psa%' OR
			LOWER(text) LIKE '%equity residential%' OR
			LOWER(text) LIKE '%eqr%' 
		THEN 'Real Estate'
		WHEN
			LOWER(text) LIKE '%nextera energy%' OR
			LOWER(text) LIKE '%nee%' OR
			LOWER(text) LIKE '%duke energy%' OR
			LOWER(text) LIKE '%duk%' OR
			LOWER(text) LIKE '%southern company%' OR
			LOWER(text) LIKE '%so%' OR
			LOWER(text) LIKE '%dominion energy%' OR
			LOWER(text) LIKE '%american electric power%' OR
			LOWER(text) LIKE '%aep%' 
		THEN 'Utilities'
		WHEN
			LOWER(text) LIKE '%alphabet%' OR
			LOWER(text) LIKE '%googl%' OR
			LOWER(text) LIKE '%meta platforms%' OR
			LOWER(text) LIKE '%meta%' OR
			LOWER(text) LIKE '%netflix%' OR
			LOWER(text) LIKE '%nflx%' OR
			LOWER(text) LIKE '%walt disney%' OR
			LOWER(text) LIKE '%dis%' OR
			LOWER(text) LIKE '%comcast%' OR
			LOWER(text) LIKE '%cmcsa%' 
		THEN 'Communication Services'
		ELSE NULL
	END AS topic
FROM (
	SELECT
		submission_id AS id,
		CONCAT(title, ' ', body) AS text,
		timestamp,
		'Submission' AS type,
		(title_pos_vader_sentiment + body_pos_vader_sentiment) / 2 AS total_pos_vader_sentiment,
		(title_neg_vader_sentiment + body_neg_vader_sentiment) / 2 AS total_neg_vader_sentiment,
		(title_compound_vader_sentiment + body_compound_vader_sentiment) / 2 AS total_compound_vader_sentiment,
		pos_finbert_sentiment AS total_pos_finbert_sentiment,
		neg_finbert_sentiment AS total_neg_finbert_sentiment,
		compound_finbert_sentiment AS total_compound_finbert_sentiment,
		upvotes,
		subreddit_id,
		submission_id,
		NULL AS comment_id,
		author_id
	FROM submission_table
	UNION ALL
	SELECT
		comment_id AS id,
		body AS text,
		timestamp,
		'Comment' AS type,
		pos_vader_sentiment AS total_pos_vader_sentiment,
		neg_vader_sentiment AS total_neg_vader_sentiment,
		compound_vader_sentiment AS total_compound_vader_sentiment,
		pos_finbert_sentiment AS total_pos_finbert_sentiment,
		neg_finbert_sentiment AS total_neg_finbert_sentiment,
		compound_finbert_sentiment AS total_compound_finbert_sentiment,
		upvotes,
		subreddit_id,
		submission_id,
		comment_id,
		author_id
	FROM comment_table
) AS AllContributions
LEFT JOIN stock_price_table 
ON CAST(AllContributions.timestamp AS DATE) = CAST(stock_price_table.timestamp AS DATE)
ORDER BY AllContributions.timestamp;
