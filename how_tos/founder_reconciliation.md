For the founder reconciliation is quite easier

1. If the source CB is available for the input domain then we only use data from crunchbase_founders. So we pull all the founders linked to this crunchbase company using the crunchbase id and we map the cols as follows.

target | source 
founders.name | crunchbase_founders.name 
founders.role | crunchbase_founders.job_title
founders.description | crunchbase_founders.description
founders.linkedin_url | crunchbase_founders.linkedin_url
source | "crunchbase"


2. Else we use traxcn_founders

target | source 
founders.name | traxcn_founders.founder_name 
founders.role | traxcn_founders.title
founders.description | traxcn_founders.description
founders.linkedin_url | traxcn_founders.profile_links
source | "traxcn"

My intuition is that to realise this mapping is very easy using SQL, so the workflow works as follows

1. We receive as input a list of domains
2. For each domain we retrieve the source of company reconciliation, either crunchbase or traxcn
for each source
    3.1 We drop all the founders linked to this company
    3.2 we run an sql command that will be different depending on the source that will update founders tables using the specified mapping
    