import requests

headers = {
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
'Accept-Encoding': 'gzip, deflate, br',
'Accept-Language': 'en-US,en;q=0.9',
'Connection': 'keep-alive',
'Cookie': 'PHPSESSID=ST-1009379-P502nRU8oNTjSCdWmN8Tvd05Y6k-FT4',
'Host': 'bhuvan-app3.nrsc.gov.in',
'Referer': 'https://bhuvan-app3.nrsc.gov.in/data/download/tools/download1/downloadlink.php?id=nices_cfri_20180102&se=NICES&sf=cfri',
'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Microsoft Edge";v="110"',
'sec-ch-ua-mobile': '?0',
'sec-ch-ua-platform': '"Windows"',
'Sec-Fetch-Dest': 'iframe',
'Sec-Fetch-Mode': 'navigate',
'Sec-Fetch-Site': 'same-origin',
'Upgrade-Insecure-Requests': '1',
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.50'
}


def download_file(url):
    local_filename = 'data/'+url.split('nices_cfri')[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True, headers=headers) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
    return local_filename

if __name__ == '__main__':
    url = f'https://bhuvan-app3.nrsc.gov.in/isroeodatadownloadutility/tiledownloadnew_cfr_new.php?f=nices_cfri_20180104.zip&se=NICES&sf=cfri&u=hitansh123'
    download_file(url)