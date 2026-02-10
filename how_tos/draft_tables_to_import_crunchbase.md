Table crunchbase companies

uuid - gen
created_at ours not crunchbase
updated_at ours not crunchbase
crunchbase_id 
name
legal_name
domain,
homepage_url,
country_code,
state_code,
region,
city,
address,
postal_code,
status,
short_description,
category_list,
category_groups_list,
num_funding_rounds,
total_funding_usd,
founded_on,
last_funding_on,
email,
phone,
facebook_url,
linkedin_url,
twitter_url,
logo_url,


Table crunchbase_funding_rounds

uuid - gen
created_at - ours
updated_at - ours
crunchbase_company_uuid FK
name,
investment_type,
announced_on,
raised_amount_usd,
post_money_valuation_usd,
investor_count,
lead_investors

Table crunchbase_founders

uuid -gen
created_at, ours
updated_at, ours
crunchbase_company_uuid, FK
name,
first_name,
last_name,
gender,
job_title,
facebook_url,
linkedin_url,
twitter_url,

