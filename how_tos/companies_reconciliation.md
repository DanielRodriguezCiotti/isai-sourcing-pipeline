The reconciliation takes as input a list of domains that must be unique

Build the reconciliation for companies tables

We need to check for each domain if they are present in crunchbase_companies and traxcn_companies
There are 3 use cases:
- They are present in both sources. In this case the record looks like this:
    logo = From crunchbase_companies.logo_url
    name = From traxcn_companies.company_name if available else crunchbase_companies.name if available else None
    domain = domain
    hq_country = From traxcn_companies.country if available else you use country_code mapping from @assets/country_codes and you turn the crunchbase_companies.country_code into a country name (using .get this way we keep at None if unknown) if available else None
    hq_city = From traxcn_companies.city if available else crunchbase_companies.city if available else None
    inc_date = From traxcn_companies.founded_year if available else crunchbase.companies.founded_on but only the year, if available else None
    description = From traxcn_companies.description if available else crunchbase.description if available, else None
    vc_current_stage = From traxcn_companies.company_stage else None
    total_amount_raised = max(traxcn_companies.total_funding_usd,crunchbase_companies.total_funding_usd) if both available else take any existing else None
    last_funding_amount = traxcn_companies.latest_funded_amount_in_usd else None
    last_funding_date = traxcn_companies.latest_funded_date else crunchbase.companies.last_funding_on
    all_investors = traxcn_companies.institutional_investors else None
    headcount = traxcn_companies.total_employee_count else None
    source = both
    all_tags = The concatenation of sector, business_models, waves, trending_themes, special_flags in traxcn_companies + the concatenation of category_list + category_groups_list from crunchbase_companies

From the tehcnical perspective we are going to use pandas
1. Pull data from crunchbase table filtering by domain as dataframe
2. Pull data from traxcn table filtering by domain as dataframe
3. We log (nb_overlapping domains, nb_domains only in CB, nb_domains only in Traxcn)
4. We build a merged df adding prefix to avoid col conflicts
5. We create one function per target column called recociliation_{target_col}
    Each one of this functions apply the logic I just explained
    PS: You can refactor one function that is pretty commont wich is taking the value from traxcn if available, else cruncbase else None
6. We .apply to create each new column
7. We keep only the columns we need 
8. We push it to supabase

You must create in src a folder db where you are going to put db client to connect to DB and the need schemas if necessary. Don't forget to use prefect logging.
Code this task called companies_recociliation in the task folder

Useful DATA is the final companies schema:
CREATE TABLE IF NOT EXISTS public.companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  
  -- Basic company information
  logo TEXT,
  name TEXT NOT NULL,
  domain TEXT NOT NULL UNIQUE,
  hq_country TEXT,
  hq_city TEXT,
  inc_date INTEGER, -- Year only (YYYY)
  description TEXT, -- max 20 words enforced at application level
  all_tags TEXT,

  -- Company metrics/info directly accessible
  vc_current_stage TEXT,
  total_amount_raised NUMERIC, -- Amount in millions USD
  last_funding_amount NUMERIC, -- Amount in millions USD
  last_funding_date TEXT, -- MM/YYYY format
  all_investors TEXT, -- max 50 words enforced at application level
  headcount INTEGER,

  -- Source of the company data
  source TEXT, -- "crunchbase" or "tracxn" or "both"

  -- Potential manual fields that can be filled by the user
  solution_fit_cg_manual INTEGER DEFAULT NULL,
  solution_fit_by_manual INTEGER DEFAULT NULL,
  business_fit_cg_manual INTEGER,
  business_fit_by_manual INTEGER,
  maturity_fit_manual INTEGER,
  equity_score_manual INTEGER,
  traction_score_manual INTEGER,
  global_fund_score_manual INTEGER
);
