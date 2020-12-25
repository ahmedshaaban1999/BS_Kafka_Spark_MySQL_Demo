from bs4 import BeautifulSoup
from kafka import KafkaProducer
import re
import requests
import sys
import pickle

def DownloadPage(link):
    header = ({'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36',\
        'Accept-Language':'en-US, en;q=0.5'})
    webPage = requests.get(link,headers=header)
    return webPage.content

def GetPrices(soup):
    try:
        if soup.find('div',attrs={'id':re.compile('olp-upd-new')}) == None:
            if soup.find('div',attrs={'id':'price'}) == None:
                exit
            return '#'.join([soup.find('div',attrs={'id':'price'}).find('span',attrs={'id':'priceblock_ourprice'}).string])

        link = soup.find('div',attrs={'id':re.compile('olp-upd-new')}).find('a',attrs={'class':'a-link-normal'})['href']
        webPage = DownloadPage('https://www.amazon.com'+link)
        miniSoup = BeautifulSoup(webPage,'lxml').body
        productPrices = [e.find('span',attrs={'class':'olpOfferPrice'}).string.strip() for e in miniSoup.find_all('div',attrs={'class':'olpOffer'})]
        return '#'.join(productPrices)
    except Exception as e:
        print(e)

def GetCategories(div):
    categories = ','.join([ e.string.strip() for e in div.find_all(['li','a'],attrs={'class':'a-link-normal'})])
    return categories

def GetProducts(soup):
    links = []
    for div in soup.find_all('div',attrs={'id':'similarities_feature_div'}):
        if div.find('ol',class_='a-carousel') != None:
            for e in div.find('ol',class_='a-carousel').find_all('a',attrs={'class':'a-link-normal'}):
                if len(e['class']) == 1:
                    links.append(e['href'])
    return links

def writeToFile(outputFile,product):
    line = ' || '.join([product['productTitle'] , product['productCategories'] , product['productPrices'] , product['productRating'] , product['productDescription']])
    outputFile.write(line)

def writeToKafka(producer,product):
    producer.send('amazon', pickle.dumps(product))    

def flush(outputFile,producer):
    outputFile.flush()
    outputFile.close()

    producer.flush()
    producer.close()

def main():
    outputFile = open('output.txt','w')
    producer = KafkaProducer()
    links = ["https://www.amazon.com/Sony-MDRZX110NC-Noise-Cancelling-Headphones/dp/B00NG57H4S/ref=pd_rhf_ee_s_gcx-rhf_0_2/132-4331926-0390640?_encoding=UTF8&pd_rd_i=B00NG57H4S&pd_rd_r=88dfb94f-b4aa-4587-b214-34818e532ce1&pd_rd_w=dIw8k&pd_rd_wg=KwVxX&pf_rd_p=2ae4ccb1-7034-4114-8654-5ba995870d70&pf_rd_r=CXZ8GYM0TJHXCXJXA90Z&psc=1&refRID=CXZ8GYM0TJHXCXJXA90Z"]
    counter = 0
    

    while any(links):
        print('proccessed ' + str(counter) + ' . links in list ' + str(len(links)))

        #To make the scrapper stop after some pages
        if counter > 50:
            break

        #wrap everything in a try/except as I didn't spent that much time investigating amazon's frontend style guide
        try:
            link = links.pop(0)
            htmlFile=DownloadPage(link)
            #For testing just download a page using a browser and read it
            #htmlFile = open('amazon.com_Sony-MDRZX110NC-Noise-Cancelling-Headphones.html','r')
            soup = BeautifulSoup(htmlFile, 'lxml')
            
            product = {}
            product['productPrices'] = GetPrices(soup)

            product['productCategories'] = GetCategories(soup.find(attrs={'id':'wayfinding-breadcrumbs_feature_div'}))
            product['productTitle'] = soup.find("span", attrs={"id":'productTitle'}).string.strip()
            product['productRating'] = soup.find(attrs={'id':'averageCustomerReviews'}).find("span", attrs={'class':'a-icon-alt'}).string.strip().split()[0]
            product['productDescription'] = soup.find('div',attrs={'id':'productDescription'}).find('p').string.strip()

            links.extend(GetProducts(soup))

            #why a file ?. It might be usefull for you later when you want to test on sending many products to kafka and you already have hit your crawling limit
            writeToFile(outputFile,product)
            writeToKafka(producer,product)

            counter = counter + 1

        except Exception as e:
            print('error in link : '+link)
            print(e)
    flush(outputFile,producer)

if __name__ == "__main__":
    main()