First think is that we are going to parse col names:
- To lower
- replace spaces by _ 
- remove thing between () except for (USD) that becomes 'in_usd'

Then the following cols must be treated as follows
SNo. DROP
Round Date -> Parse date using function
Round Amount (USD) -> if int okay, if not ex "19,775,112.042" remove everything after . and replace , by nothing to create int, else none
Institutional Investors -> comma2list
Angel Investors -> comma2list
Lead Investor -> comma2list
Facilitators -> comma2list
Founded Year -> to int
Practice Areas -> comma2list
Feed Name -> comma2list
Business Models -> comma2list


comma2list : There must be a function that turns string containing comas into a list
parsedate : This function allows to parse dates in the following formats 
   Oct 16
 2019 -> DD/MM/YYYY
   2015 -> 01/01/YYYY
   Jan 2023 -> 01/MM/YYYY

If there is nothing explicit about a col keep it as it is