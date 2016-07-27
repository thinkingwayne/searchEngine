import urllib2
from urlparse import urljoin
from BeautifulSoup import *
import sqlite3  as sqlite
import nn

mynet = nn.searchnet('nn.db')

#  create a list of words to ignore
ignorewords = set(['the', 'of', 'to', 'and', 'a', 'in','is' 'it'])


class crawler:

    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def dbcommit(self):
        self.con.commit()

    def getentryid(self, table, field, value, createnew = True):
        cur = self.con.execute(
            "select rowid from %s where %s = '%s'" % (table,field,value))
        res = cur.fetchone()
        if res == None:
            cur = self.con.execute(
                "insert into %s (%s) values ('%s')" % (table, field, value))
            return cur.lastrowid
        else:
            return res[0]

    def addtoindex(self, url, soup):
        if self.isindexed(url):
            return
        print 'indexing %s' % url
        # get the individual word
        text = self.gettextonly(soup)
        words = self.separatewords(text)
        # get the URL id
        urlid = self.getentryid('urllist','url',url)

        # link each word to this url
        for i in range(len(words)):
            word = words[i]
            if word in ignorewords:
                continue
            wordid = self.getentryid('wordlist','word',word)
            self.con.execute("insert into wordlocation(urlid,wordid,location) values (%d,%d,%d)" % (urlid,wordid,i))

    def gettextonly(self, soup):
        v = soup.string 
        if v == None:
            c = soup.contents
            resulttext = ''
            for t in c:
                subtext = self.gettextonly(t)
                resulttext += subtext+'\n'
            return resulttext
        else:
            return v.strip()


    def separatewords(self, text):
        splitters = re.compile('\\W*')
        return [s.lower() for s in splitters.split(text) if s!='']


    # Return true if this url is already indexed
    def isindexed(self, url):
        u = self.con.execute(
            "select rowid from urllist where url='%s'" % url).fetchone()
        if u!=None:
            v = self.con.execute(
                'select * from wordlocation where urlid = %d' % u[0]).fetchone()
            if v != None:
                return True
        return False

    # add a link between two pages
    def addlinkref(self, urlFrom, urlTo, linkText):
        pass

    # starting with a list of pages, do a breadth first
    # search to the given depth, indexing pages as we go
    def crawl(self, pages, depth=2):
        for i in xrange(depth):
            newpages = set()
            for page in pages:
                try:
                    c = urllib2.urlopen(page)
                except:
                    print "Could not open %s" % page
                    continue
                soup = BeautifulSoup(c.read())
                self.addtoindex(page, soup)

                links = soup('a')
                for link in links:
                    if ('href' in dict(link.attrs)):
                        url = urljoin(page, link['href'])
                        if url.find("'") != -1:
                            continue
                        url = url.split('#')[0]  # remove location portion
                        if url[0:4] == 'http' and not self.isindexed(url):
                            newpages.add(url)
                        linkText = self.gettextonly(link)
                        self.addlinkref(page, url, linkText)
                self.dbcommit()
            pages = newpages

    # create the database tables
    def createindextables(self):
        # self.con.execute('drop table worldlocation')
        # self.con.execute('create table urllist(url)')
        # self.con.execute('create table wordlist(word)')
        # self.con.execute('create table wordlocation(urlid,wordid,location)')
        # self.con.execute('create table link(fromid integer, toid integer)')
        # self.con.execute('create table linkwords(wordid, linkid)')
        # self.con.execute('create index wordidx on wordlist(word)')
        # self.con.execute('create index urlidx on urllist(url)')
        # self.con.execute('create index wordurlidx on wordlocation(wordid)')
        # self.con.execute('create index urltoidx on link(toid)')
        # self.con.execute('create index urlfromidx on link(fromid)')
        self.dbcommit()

class searcher:
    def __init__(self,dbname):
        self.con = sqlite.connect(dbname)
    def __del__(self):
        self.con.close()
    def getmatchrows(self,q):
        # strings to build the query
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []
        # split the words by spaces
        words = q.split()
        tablenumber = 0

        for word in words:
            # get the word id
            wordrow = self.con.execute(
                "select rowid from wordlist where word = '%s'" % word).fetchone()
            if wordrow != None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber > 0:
                    tablelist += ','
                    clauselist += ' and '
                    clauselist += ' w%d.urlid=w%d.urlid and ' % (tablenumber-1,tablenumber)
                fieldlist+=',w%d.location' % tablenumber
                tablelist+='wordlocation w%d' % tablenumber
                clauselist+='w%d.wordid=%d' % (tablenumber,wordid)
                tablenumber+=1
        # create the query from the separate parts
        fullquery = 'select %s from %s where %s' % (fieldlist,tablelist,clauselist)
        cur = self.con.execute(fullquery)
        rows = [row for row in cur]
        return rows,wordids
    
    def getscorelist(self, rows, wordids):
        totalscores = dict([(row[0],0) for row in rows])
        # This is where you'll later put the scoring functions
        weigths=[(1.5, self.locationscore(rows)),
                 (1.0, self.frequencyscore(rows))]
        for (weigth,scores) in weigths:
            for url in totalscores:
                totalscores[url]+= weigth*scores[url]
        return totalscores
    def geturlname(self,id):
        return self.con.execute(
            "select url from urllist where rowid = %d " % id ).fetchone()
    
    def query(self,q):
        rows,wordids = self.getmatchrows(q)
        scores = self.getscorelist(rows,wordids)
        rankedscores = sorted([(score,url) for (url,score) in scores.items()],reverse =1)
        for (score,urlid) in rankedscores[0:10]:
            print '%f\t%s' % (score, self.geturlname(urlid))

        return worids, [r[1] for r in rankedscores[0:10]]
    
    def normalizescores(self,scores,smallIsBetter = 0):
        vsmall = 0.00001
        if smallIsBetter:
            minscore = min(scores.values())
            return dict([(u,float(minscore)/max(vsmall,l)) for \
                (u,l) in scores.items()])
        else:
            maxscore = max(scores.values())
            if maxscore == 0: maxscore=vsmall
            return dict([(u,float(l)/maxscore) for\
                (u,l) in scores.items()])
    def frequencyscore(self, rows):
        count = dict([(row[0],0) for row in rows])
        for row in rows:
            count[row[0]] += 1
        return self.normalizescores(count)
    def locationscore(self,rows):
        locations = dict([(row[0],1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc<locations[row[0]]:
                locations[row[0]] = loc
        return self.normalizescores(locations,smallIsBetter=1)

    def nnscore(self, rows, wordids):
        urlids = [urlid for urlid in set([row[0] for row in rows])]
        nnres = myset.getresult(wordids, urlids)
        scores = dict([(urlids[i], nnres[i]) for i in range(len(urlids))])
        return self.normalizescores(scores)
# pagelist = ['http://kiwitobes.com']
# crawler = crawler('searchindex.db')
# crawler.createindextables()
# crawler.crawl(pagelist)
# rows = [row for row in crawler.con.execute(
#     'select rowid from wordlocation where wordid = 3')]
# print rows
e=searcher('searchindex.db')
e.query('make little which')


