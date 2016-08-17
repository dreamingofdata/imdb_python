# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 21:29:50 2016
Last updated: 2016-08-15

@author: Bill

The objective of this program is to connect to a personal GoogleDoc spreadsheet
 of movies I have collected and their associated IMDB id. It then webscrapes IMDB
 to gata data elements of interest (title, poster image, synopsis, top stars, etc).
 This data will then be added to an SQLite database for future querying and
 extraction.
"""

# http://docs.python-requests.org/en/master/
import requests  

# https://www.crummy.com/software/BeautifulSoup/
from bs4 import BeautifulSoup

# https://github.com/burnash/gspread
import gspread

# https://developers.google.com/api-client-library/python/guide/aaa_oauth
from oauth2client.service_account import ServiceAccountCredentials

# Other standard Python libraries
import sqlite3
import urllib
import csv
from datetime import datetime
import sys

def main():
    #Grab list of movies from GoogleSheet
    #(pull from local text file for now)
    myMovies = getMyMovies()

    #Establish connection to SQLite db
    dbConnection = getDBConnection()

    #Process movie list
    processMovies(myMovies, dbConnection)

    #Use the data in the db to regenerate output HTML
    writeCatalog(dbConnection)

def getMyMovies():
    """
    Data is stored as a pair of data elements:
     - IMDB ID
     - Date added to collection
     See http://gspread.readthedocs.io/en/latest/oauth2.html
    """
    #myMovies = list(csv.reader(open('imdb_titles.txt'), delimiter='\t'))
    scope = ['https://spreadsheets.google.com/feeds']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('MyMovies-27d160dfe8d3.json', scope)
    gc = gspread.authorize(credentials)

    worksheet = gc.open("MyMovies").sheet1
    content = worksheet.get_all_values()

    headers = content[0]

    myMovies = []
    for movie in content[1:]:
        #Extract the data elements
        imdb_id = movie[2]
        date_added = movie[3]
        myMovies.append([imdb_id, date_added])

    return myMovies

def getDBConnection():
    return sqlite3.connect('imdb_collection.sqlite')

def processMovies(movieList, dbConnection):
    """
    The main processing loop. For each record found in movieList, check first to
    see if the movie ID already exists in the database, if not, look it up in
    IMDB and add a record.
    """
    cursor = dbConnection.cursor()
    newMovieCounter = 0
    
    for line in movieList:
        imdb_id = line[0].strip()
        #Not every movie as an imdb ID - skip these records
        if imdb_id == "":
            continue
        date_added = datetime.strptime(line[1], '%m/%d/%Y')
    
        #Check to see if ID already exists in DB
        cursor.execute("SELECT ID FROM imdb_data WHERE ID= (?)", (imdb_id,))

        try:
            data = cursor.fetchone()[0]
            #Record exists - skip to next movie
            continue
        except:
            #Record not found, continue onto movie processing
            print("Exception occured", sys.exc_info()[0])

        result = extract_imdb_info(imdb_id)
        cursor.execute('''INSERT INTO imdb_data (ID, Title, Year, Categories,
                                                 Director, Actors, User_Rating,
                                                 Summary, MPAA_Rating, Date_Added) 
                          VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ) ''', 
                       (result["imdb_id"],
                        result["title"],
                        result["year"], 
                        ', '.join(result["categories"]),
                        result["director"], 
                        ', '.join(result["actors"]),
                        result["user_rating"], 
                        result["summary"],
                        result['MPAA_rating'],
                        date_added))
        dbConnection.commit()

        newMovieCounter += 1

        #Grab the image file and store it into /covers
        url = result['img_src']
        if (url == "nocover" or url == ""):
            # Image is not available, use a default image for this imdb_id
            url = "http://ia.media-imdb.com/images/G/01/imdb/images/nopicture/180x268/unknown-3315334037._CB288986052_.png"
        #print("image URL: ", url)
        fileName = 'covers\\' + imdb_id + '.jpg'
        req = requests.get(url)
        file = open(fileName, 'wb')
        for chunk in req.iter_content(100000):
            file.write(chunk)
        file.close()

    print("%s movies have been added"%(newMovieCounter))

def extract_imdb_info(imdb_id):
    """
    Handles the pull of data from IMDB for a given movie ID
    Returns a dictionary of objects pulled from the web page
    """
    url = "http://www.imdb.com/title/" + imdb_id
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as response:
        html = response.read()
    
    soup = BeautifulSoup(html, 'html.parser')
    #------------------------------------------------------
    #Get image source URL
    #------------------------------------------------------
    img_src = ''
    image_div = soup.find('div', attrs={'class':'poster'})
    if image_div == None:
        img_src == None
    else:
        a_tag = image_div.a
        if a_tag == None:
            img_src = "nocover"
        else:
           img_tag = a_tag.img
           img_src = img_tag['src']
        
    #------------------------------------------------------
    #Get title
    #------------------------------------------------------
    title_class = soup.find("div", attrs={"class":"title_wrapper"})
    title_h1 = title_class.find("h1", attrs={"itemprop":"name"})
    title = title_h1.contents[0].strip()
    
    #------------------------------------------------------
    #Get year
    #------------------------------------------------------
    year_span = soup.find("span", attrs={"id":"titleYear"})
    if year_span == None:
        year = "-"
    else:
        year = year_span.find("a").contents[0] 
    
    #------------------------------------------------------
    #Get movie summary
    #------------------------------------------------------
    summary_div = soup.find("div", attrs={"itemprop":"description"})
    if summary_div.string == None:
        summary = ""
    else:
        summary = summary_div.string.strip()
    
    #------------------------------------------------------
    #Get categories
    #------------------------------------------------------
    categories = []
    for category in soup.find_all("span", attrs={"class":"itemprop", "itemprop":"genre"}):
       categories.append(category.string)
    
    #------------------------------------------------------
    #Get director
    #------------------------------------------------------
    director_span = soup.find("span", attrs={"itemprop":"director"})
    if director_span == None:
        director = ""
    else:
        director = director_span.find("span", attrs={"itemprop":"name"}).string
    
    #------------------------------------------------------
    #Get top actors
    #------------------------------------------------------
    actors = []
    for actor_span in soup.find_all("span", attrs={"itemprop":"actors"}):
        actor = actor_span.find("span", attrs={"class":"itemprop"})
        actors.append(actor.string)
    
    #------------------------------------------------------
    #Get user rating
    #------------------------------------------------------
    user_rating_span = soup.find("span", attrs={"itemprop":"ratingValue"})
    if user_rating_span == None:
        user_rating = None
    else:
        user_rating = float(user_rating_span.string)

    #------------------------------------------------------
    #Get MPAA rating
    #------------------------------------------------------
    MPAA_rating_span = soup.find("span", attrs={"itemprop":"contentRating"})
    if MPAA_rating_span == None:
        MPAA_rating = 'no rating'
    else:
        MPAA_rating = MPAA_rating_span.string.strip()

    
    imdb_info = {}
    imdb_info = {"imdb_id":imdb_id,
                 "title":title,
                 "year":year,
                 "categories":categories,
                 "director":director,
                 "actors":actors,
                 "user_rating":user_rating,
                 "MPAA_rating":MPAA_rating,                 
                 "summary":summary,
                 "img_src":img_src}
    return imdb_info

def writeCatalog(dbConnection):
    fout = open('mymovies.html', 'w' )
    fout.write("<!DOCTYPE html>\n")
    fout.write("<html>\n")
    fout.write("<head>\n")
    fout.write("<style>\n")
    fout.write("table {border-collapse: collapse;}\n")
    fout.write("span.title {font-family: 'Arial'; font-weight: bold;font-size: 22px;}\n")
    fout.write("span.year {font-family: 'Arial'; font-weight: normal;font-size: 14px;}\n")
    fout.write("span.infoheading {font-family: 'Arial'; font-weight: bold;font-size: 14px;}\n")
    fout.write("span.infodetail {font-family: 'Arial'; font-weight: normal;font-size: 14px;}\n")
    fout.write("span.plot {font-family: 'Arial'; font-weight: normal; font-style: italic; font-size: 14px;}\n")
    fout.write("</style>\n")
    fout.write("<metacontent='text/html; charset=UTF-8' http-equiv='content-type'>\n")
    fout.write("<title>BillsMovies</title>\n")
    fout.write("</head>\n")
    fout.write("<table border width='600'>\n")
    fout.write("<tbody>\n")

    with dbConnection:
        dbConnection.row_factory = sqlite3.Row
        cur = dbConnection.cursor()
        cur.execute('SELECT ID, Title, Year, Categories, Director, Actors, User_Rating, MPAA_Rating, Summary, Date_Added FROM imdb_data' )
        rows = cur.fetchall()
        for row in rows:
            fout.write("<tr>\n")
            line = "<td  style='width: 250px;'><img  alt=''  src='covers/%s.jpg'><br>\n"%row["ID"]
            fout.write(line)
            fout.write("</td>\n")
            fout.write("<td  style='width: 100%; vertical-align: top;'>\n")
            fout.write("<table  style='width: 100%;'  border='0'>\n")
            fout.write("<tbody>\n")
            fout.write("<tr>\n")
            line = "<td colspan='2'><span class='title'>%s</span></td>\n"%row["Title"]
            fout.write(line)
            fout.write("</tr>\n")
            fout.write("<tr>\n")
            fout.write("<td><span class='infoheading'>Year:</span></td>\n")
            line = "<td><span class='infodetail'>%s</span></td>\n"%row["Year"]
            fout.write(line)
            fout.write("</tr>\n")
            fout.write("<tr>\n")
            fout.write("<td><span class='infoheading'>Categories:</span> </td>\n")
            line = "<td><span class='infodetail'>%s</span></td>\n"%row["Categories"]
            fout.write(line)
            fout.write("</tr>\n")
            fout.write("<tr>\n")
            fout.write("<td><span class='infoheading'>Directed by:</span></td>\n")
            line = "<td><span class='infodetail'>%s</span></td>\n"%row["Director"]
            fout.write(line)
            fout.write("</tr>\n")
            fout.write("<tr style='vertical-align: top;'>\n")
            fout.write("<td><span class='infoheading'>Top cast:</span></td>\n")
            line="<td><span class='infodetail'>%s</span></td>\n"%row["Actors"]
            fout.write(line)
            fout.write("</tr>\n")
            fout.write("<tr>\n")
            fout.write("<td style='white-space: nowrap;'><span class='infoheading'>IMDB User rating</span></td>\n")
            line = "<td><span class='infodetail'>%s</span></td>\n"%row["User_Rating"]
            fout.write(line)
            fout.write("</tr>\n")
            fout.write("<tr>\n")
            fout.write("<td style='white-space: nowrap;'><span class='infoheading'>MPAA rating</span></td>\n")
            line = "<td><span class='infodetail'>%s</span></td>\n"%row["MPAA_Rating"]
            fout.write(line)
            fout.write("</tr>\n")
            date_added = row["Date_Added"][:10]
            fout.write("<tr>\n")
            fout.write("<td><span class='infoheading'>Added</span></td>\n")
            line = "<td><span class='infodetail'>%s</span></td>\n"%date_added
            fout.write(line)
            fout.write("</tr>\n")
            fout.write("<tr>\n")
            line = "<td colspan='2'><span class='plot'>%s</span></td>\n"%row["Summary"]
            fout.write(line)
            fout.write("</tr>\n")
            fout.write("</tbody>\n")
            fout.write("</table>\n")
            fout.write("</td>\n")
            fout.write("</tr>\n")
    fout.write("</tbody>\n")
    fout.write("</table>\n")
    fout.write("<body>\n")
    fout.write("</html>")
    fout.close()

    print("Catalog written")

if __name__ == '__main__':
    main()
