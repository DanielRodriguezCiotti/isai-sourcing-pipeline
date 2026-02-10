First think is that we are going to parse col names:
- To lower
- replace spaces by _ 
- remove thing between () except for (USD) that becomes 'in_usd'
- remove al ' 
- remove ': TRUE' in Special Flags col

Then the following cols must be treated as follows

SNo. DROP
Founded Year -> becomes an integere
Sector (Pratice Area & Feed) -> we use comma2list to parse it and it becomes a list of text
Team Background -> we use comma2list to parse it and it becomes a list of text
Waves -> we use comma2list to parse it and it becomes a list of text
Trending Themes -> we use comma2list to parse it and it becomes a list of text
Is Funded -> should become a bool true or false
Latest Funded Date -> parse the date
Institutional Investors -> we use comma2list to parse it and it becomes a list of text
Angel Investors -> we use comma2list to parse it and it becomes a list of text
Key People Email Ids -> we use comma2list to parse it and it becomes a list of text
Links to Key People Profiles -> we use comma2list to parse it and it becomes a list of text
Acquisition List -> we use comma2list to parse it and it becomes a list of text
Editor's Rated Date -> parse date from format 2019-10-16 
Soonicorn Club Status DROP
Soonicorn Club Event Date DROP
Company Emails  -> we use comma2list to parse it and it becomes a list of text
Website Status Last Updated -> parse date from format 2019-10-16 
Date Added -> parse date from format 2019-10-16 
Is Deadpooled -> to bool

comma2list : There must be a function that turns string containing comas into a list
parsedate : This function allows to parse dates in the following formats 
   Oct 16, 2019 -> DD/MM/YYYY
   2015 -> 01/01/YYYY
   Jan 2023 -> 01/MM/YYYY

If there is nothing explicit about a col keep it as it is