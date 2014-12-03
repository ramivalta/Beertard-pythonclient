import ddp
import time
import json, sqlite3
import urllib2
import logging
import requests
import re
from twisted.internet import reactor
from sets import Set

log = logging.getLogger('olutta')
log.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
log.addHandler(ch)

# DDP client
# Create a client, passing the URL of the server.
client = ddp.ConcurrentDDPClient('ws://oluttutka.meteor.com/websocket')

# Once started, the client will maintain a connection to the server.
client.start()

untappdBaseUrl = "https://api.untappd.com/v4"
venueApi = "/venue/info/"
authToken = ""

def getJsonDataFromUrl(url):
	response = requests.get(url, timeout=60)
	global data
	try:
		data = response.json()
	except:
		log.error("Unable to parse json data from response")

	return data

def fetch_venue(venueId):
	apiUrl = untappdBaseUrl+venueApi+venueId+authToken

#	apiUrl = "https://www.kimonolabs.com/api/ekaoe724?apikey=1Sb5Ve0TMHGlD35pKOeREG7FNRIMO6ZO&kimpath2=%s" %(venueId)
	log.info("Getting venue data from apiUrl " + apiUrl)
	response = requests.get(apiUrl)

	data = response.json()
	venueInfo = data['response']['venue']
	log.info("Venue name " + venueInfo['venue_name'])

	parsedVenueData = {'untappdId':venueId, 'name':venueInfo['venue_name'], 'location':venueInfo['location'], 'contact':venueInfo['contact']}

	log.info("Parsed venuedata " + json.dumps(parsedVenueData))

	#return 0

	savedVenueId = addVenueInfo(venueId, venueInfo['venue_name'])
	venue = {'_id':savedVenueId, 'untappdId': venueId, 'name': venueInfo['venue_name']}
	parse_venue_beers2(venue, venueInfo['checkins']['items'])
	
	time.sleep(10)
#	venueInfo = data['results']['venueDetails'][0]
	

	#log.info("Venue info " + venueInfo)
#	log.info("Venue name " + venueInfo['venueName'])
#	log.info("Venue type " + venueInfo['venueType'])
#	log.info("Venue address " + venueInfo['venueAddress']['text'])

#	savedVenueId = addVenueInfo(venueId, venueInfo['venueName'], venueInfo['venueType'], venueInfo['venueAddress']['text'])
#	venue = {'_id':savedVenueId, 'untappdId': venueId, 'name': venueInfo['venueName'], 'type': venueInfo['venueType'], 'address': venueInfo['venueAddress']['text']}
#	parse_venue_beers(venue, data['results']['beerList'])


	return 0

def findVenue(venueUntappdId):
	venue = None
	future = client.call('findVenueWithUntappdId', venueUntappdId)

        result_message = future.get()

        # Check if an error occured else print the result.
        if result_message.has_error():
                print result_message.error
        else:
                venue = result_message.result
	return venue

def addVenue(venue):
	future = client.call('addOrUpdateVenue', venue)

def addVenueInfo(venueUntappdId, venueName):
	existingVenue = findVenue(venueUntappdId)
	if existingVenue is not None:
		print 'Venue already exists. Not adding it again'
		return existingVenue['_id']
	# ... Do something with it ...
	future = client.call('addVenue', {'untappdId':venueUntappdId, 'name':venueName})
	#future = client.call('addVenue', {'untappdId':venueUntappdId, 'name':venueName, 'type':venueType, 'address':venueAddress});
	#future = client.call('Beers.insert', 'insert');
	# Block until the result message is received.
	result_message = future.get()

	# Check if an error occured else print the result.
	if result_message.has_error():
        	print result_message.error
	else:
        	return result_message.result
	
def parse_venue_beers2(venue, beers):
	for beer in beers:
		beerInfo = beer['beer']
		breweryInfo = beer['brewery']
		log.info("Beerinfo is " + json.dumps(beerInfo))
		beer_description = ""
		if 'beer_description' in beerInfo:
			beer_description = beerInfo['beer_description']

		beerToAdd = {'untappdId': beerInfo['bid'], 'name': beerInfo['beer_name'], 'abv':beerInfo['beer_abv'], 'description':beer_description, 'style':beerInfo['beer_style'], 'brewery':breweryInfo['brewery_name'], 'country':breweryInfo['country_name'], 'last_seen': beer['created_at']}
		log.info("Beer to add " + json.dumps(beerToAdd))
		#return 0
		log.info("Adding beer " + json.dumps(beerToAdd))
		future = client.call('addUntappdBeer', beerToAdd, venue)
	        result_message = future.get()

        	# Check if an error occured else print the result.
	        if result_message.has_error():
        	        print result_message.error
	        else:
        	        print result_message.result


def parse_venue_beers(venue, beers):
	for beer in beers:
		log.info("Beer name " + beer['beer']['text'])
		log.info("Beer url " + beer['beer']['href'])
		urlParts = beer['beer']['href'].split('/')
		#parse_brewery_info(beer['brewery'])
		parse_beer_info(venue, urlParts[len(urlParts)-2], urlParts[len(urlParts)-1], beer['timestamp'])
		#break

def parse_beer_info(venue, beerName, beerId, timestamp):
	time.sleep(10)
	apiUrl = 'https://www.kimonolabs.com/api/bbkk8jew?apikey=1Sb5Ve0TMHGlD35pKOeREG7FNRIMO6ZO&kimpath2=%s&kimpath3=%s' %(beerName,beerId)
	log.info("Fetch beer info using apiUrl " + apiUrl)
	data = getJsonDataFromUrl(apiUrl)
	beerInfo = data['results']['beerInfo'][0]	
	log.info("Beer style " + beerInfo['style'])

	abv = None
	abvStr = re.findall("\d*\.\d+|\d+", beerInfo['abv'])
	if abvStr:
		abv = abvStr[0]
		log.info("Beer ABV " + abvStr[0])

	ibuStr = re.findall("\d*\.\d+|\d+", beerInfo['ibu'])

	ibu = None
	if ibuStr:
		ibu = ibuStr[0]
		log.info("Beer IBU " + ibuStr[0])
	beer = {'untappdId':beerId,'name':beerName,'abv':abv,'ibu':ibu,'style':beerInfo['style'],'name':beerInfo['name']}
	#log.info("trying to add beer " + beer)
	#future = client.call('addBeer', beer)
	future = client.call('addUntappdBeer', beer, venue)
        result_message = future.get()

        # Check if an error occured else print the result.
        if result_message.has_error():
                print result_message.error
        else:
                return result_message.result

def fetch_olutta():

        badBeer = ['karhu', 'budvar', 'stella', 'guinness','kilkenny']

        apiUrl = "https://www.kimonolabs.com/api/3y4nos3g?apikey=1Sb5Ve0TMHGlD35pKOeREG7FNRIMO6ZO"
        #response = bot.get_url(apiUrl)
	log.info("Tryin to get from " + apiUrl)
	response = requests.get(apiUrl)

        try:
                data = response.json()
        except:
                log.debug("Couldn't parse JSON.")
		return 1

        log.debug(data['results']['collection1'])

        tyypit = data['results']['collection1']

	result_return = Set()

	onksPaha = False

        for tyyppi in tyypit:
                if tyyppi['tyyppi'] == "DRAUGHT BEERS":
			for bisse in tyyppi['nimi']:
				for paha in badBeer:
					if paha.lower() in bisse.lower():
						onksPaha = True
						continue
				if not onksPaha:
					result_return.add(bisse)
				onksPaha = False
					

                        #bisset = " | ".join( tyyppi['nimi'])
                        #bot.say(channel, bisset, 450)
			bisset2 = " | ".join(result_return)
			bot.say(channel, bisset2, 450)
			#bot.say(channel, "Karhu III, Suomi 4.6% *Lager | Karhu IV, Suomi 5.3% *Lager | Stella Artois, Belgia 5% | Budvar Premium, Tsekki 5% | Budvar Dark, Tsekki 4.7% | Kilkenny, Irlanti 4.3% *Red Ale | Guinness, Irlanti 4.3% *Stout | Hoegaarden, Belgia 5% *White Beer | Harju Pale Ale, Suomi 5.2% *Pale ale | Fullers London Pride, Englanti, 4,7 % *PremiumAle | Plevnan Siperia, Suomi, 8,0 % *Stout | Brussels Beer Delta Ipa, Belgia, 6,0 % *Ipa | Plevnan Oktober, Suomi, 5,9% * Luomu lager | Nogne # 100, Norja, 10% *Barley Wine-ale | Malmgord Emmer Ipa, Suomi, 6,2% *Ipa | Hiisin&Maistilan Two Bad, Suomi, 8,0% *Belge Ale | Brewdog, Jack Hammer Skotlanti, 7,2% *Ipa")
			#bot.say(channel, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 450)


if __name__ == "__main__":
	#fetch_venue('350048')
	untappdVenueIds = ['350048', '31417', '31668', '31552', '421773']
	#untappdVenueIds = ['31552', '421773']
	for untappdVenueId in untappdVenueIds:
		fetch_venue(untappdVenueId)
	client.stop()
	client.join()
