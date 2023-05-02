import requests
import time
import csv
import random
import concurrent.futures


from bs4 import BeautifulSoup

# global headers to be used for requests
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'}

MAX_THREADS = 10


def extract_movie_details(movie_link):
    time.sleep(random.uniform(0, 0.2))
    response = BeautifulSoup(requests.get(movie_link, headers=headers).content, 'html.parser')
    movie_soup = response

    if movie_soup is not None:
        title = None
        date = None

        movie_data = movie_soup.find('div', attrs={'class': 'sc-52d569c6-0'})
        if movie_data is not None:
            title = movie_data.find('span', attrs={'class': 'sc-afe43def-1'}).get_text() if movie_data.find('span', attrs={'class': 'sc-afe43def-1'}) else None
            date = movie_data.find('a', attrs={'class': 'ipc-link'}).get_text().strip() if movie_data.find('a', attrs={'class': 'ipc-link'}) else None

        rating = movie_soup.find('span', attrs={'class': 'sc-bde20123-1'}).get_text() if movie_soup.find(
            'span', attrs={'class': 'sc-bde20123-1'}) else None

        plot_text = movie_soup.find('span', attrs={'class': 'sc-5f699a2-2'}).get_text().strip() if movie_soup.find(
            'span', attrs={'class': 'sc-5f699a2-2'}) else None

        with open('/Users/rvf_alves/Documents/Comp/CursoPython/Python_avancado/EBAC-python-concurrency/movies_multi.csv', mode='a') as file:
            movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if all([title, date, rating, plot_text]):
                #print(title, date, rating, plot_text)
                movie_writer.writerow([title, date, rating, plot_text])


def extract_movies(soup):
    movies_table = soup.find('table', attrs={'data-caller-name': 'chart-moviemeter'}).find('tbody')
    movies_table_rows = movies_table.find_all('tr')
    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]

    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details, movie_links)


def main():
    start_time = time.time()

    # IMDB Most Popular Movies - 100 movies
    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Main function to extract the 100 movies from IMDB Most Popular Movies
    extract_movies(soup)

    end_time = time.time()
    print('Total time taken: ', end_time - start_time)


if __name__ == '__main__':
    main()