For question 1, I will use the reduce-side join because it scales better 
when we know both input files can be very large. In our case, the 
hostname_country.csv file could possibly be very large. So using a reduce-side
join is the best move in this case. To accomplish the first question, I needed ]
three jobs. The first one had two mappers. The first one emits (hostname, Country).
The second mapper emits (Hostname, 1). The output is a file with all the countries 
and the number of times a request was sent from a given country. I use the second
job (report1b) to add up all the requests for a given country. Finally, the third
job (report1c) handles the sorting where I had to negate the counts so that 
my sort would be in descending order as the instructions ask.

For question 2, the biggest key to solving this one is making use of
more complex data types. I used UrlCountryPair and CountryCountPair
so that I could store and manipulate data more effectively throughout
the map-reduces jobs. The other thing of note was that I had to negate 
the comparator in the CountryCountPair so that things were sorted in the
correct order. 