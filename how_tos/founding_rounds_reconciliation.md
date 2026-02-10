For the founding rounds reconciliation is easy too, we are going to put both sources in the table

So basically you pull everything from both sources and push it to the founding rounds table

target | source 
founding_rounds.date | crunchbase_founding_rounds.announced_on 
founding_rounds.stage | crunchbase_founding_rounds.investment_type
founding_rounds.amount | crunchbase_founding_rounds.raised_amount_usd
founding_rounds.lead_investors | crunchbase_founding_rounds.lead_investors
founding_rounds.all_investors | also crunchbase_founding_rounds.lead_investors
founding_rounds.source | "crunchbase"


target | source 
founding_rounds.date | traxcn.founding_rounds.round_date 
founding_rounds.stage | traxcn.founding_rounds.round_name
founding_rounds.amount | traxcn.founding_rounds.round_amount_in_usd
founding_rounds.lead_investors | traxcn.founding_rounds.lead_investor
founding_rounds.all_investors | traxcn.founding_rounds.institutional_investors
founding_rounds.source | "traxcn"

So push both sources 

Here you have the table schemas
CREATE TABLE IF NOT EXISTS public.funding_rounds (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),

  company_id UUID NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
  date DATE,
  stage TEXT, -- e.g. Seed, Series A, Series B…
  amount NUMERIC, -- Amount in millions
  lead_investors TEXT[],
  all_investors TEXT[],
  source TEXT -- "cb" or "tracxn"
  CONSTRAINT uq_funding_rounds_identity UNIQUE (date, company_id, stage, source)
);

CREATE TABLE IF NOT EXISTS traxcn_funding_rounds (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    round_date DATE,
    company_name TEXT NOT NULL,
    domain_name TEXT NOT NULL,
    round_name TEXT,
    round_amount_in_usd NUMERIC(15, 2),
    round_pre_money_valuation_in_usd NUMERIC(15, 2),
    round_post_money_valuation_in_usd NUMERIC(15, 2),
    round_trailing_12m_revenue_in_usd NUMERIC(15, 2),
    institutional_investors TEXT[],
    angel_investors TEXT[],
    lead_investor TEXT[],
    facilitators TEXT[],
    total_funding_in_usd NUMERIC(15, 2),
    round_revenue_multiple NUMERIC(10, 2),
    overview TEXT,
    founded_year INTEGER,
    country TEXT,
    state TEXT,
    city TEXT,
    practice_areas TEXT[],
    feed_name TEXT[],
    business_models TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_company FOREIGN KEY (domain_name)
        REFERENCES traxcn_companies(domain_name)
        ON DELETE CASCADE,
    CONSTRAINT uq_traxcn_funding_identity UNIQUE (round_date, domain_name, round_name)
);

CREATE TABLE IF NOT EXISTS crunchbase_funding_rounds (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    crunchbase_company_uuid TEXT NOT NULL,
    name TEXT,
    investment_type TEXT,
    announced_on DATE,
    raised_amount_usd NUMERIC(15, 2),
    post_money_valuation_usd NUMERIC(15, 2),
    investor_count INTEGER,
    lead_investors TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_funding_round_company FOREIGN KEY (crunchbase_company_uuid)
        REFERENCES crunchbase_companies(crunchbase_id)
        ON DELETE CASCADE,
    CONSTRAINT uq_crunchbase_funding_identity UNIQUE (announced_on, crunchbase_company_uuid, investment_type)
);
