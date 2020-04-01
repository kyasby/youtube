import os
import time
import requests
import pandas as pd
import datetime
import logging
from logging import getLogger, StreamHandler, Formatter,FileHandler
from tkinter import messagebox


API_KEY = ''
base_url = 'https://www.googleapis.com/youtube/v3'
logger = getLogger("logger").getChild("sub")

def make_ch_date(channel, middle, last_dt, ch_name, api):

    CHANNEL_ID = channel
    middle = middle     #str
    last_dt = last_dt   #str
    MAX = 50
    API_KEY = api
    ch_url = base_url + '/search?key=%s&channelId=%s&publishedAfter=%s&publishedBefore=%s&fields=nextPageToken,items(id,snippet(publishedAt,title))&part=id,snippet&order=date&maxResults=%i'    
    ch_infos = []
    flag = False
    ch_consume = 0
    description_consume = 0
    logger.info("make_ch_data:初期定義:API,CHANNEL_ID,DATES,URL")
    
    try:#フォルダに移動→ID.csv取得
        try:
            videos = pd.read_csv(ch_name + '\\' + CHANNEL_ID + '.csv')
            logger.info("フォルダに移動し"+ ch_name + "のチャンネルCSV取得成功")
        except:
            #フォルダがない場合作る
            logger.info("フォルダがない！！！")
            os.mkdir(ch_name)
            logger.info(ch_name + "のフォルダ作成に成功")
            videos = pd.read_csv(ch_name + '\\' + CHANNEL_ID + '.csv')
            logger.info(ch_name + "のチャンネルCSV取得に成功")
    except:#ID.csvがない場合作る
        videos = pd.DataFrame(ch_infos, columns=['videoId', 'title','description', 'publishedAt'])
        videos.to_csv(ch_name + '\\' + CHANNEL_ID + '.csv', index=None)
        logger.info("チャンネルCSV作成に成功")
    
    while True:
        ch_response = requests.get(ch_url % (API_KEY, CHANNEL_ID, middle, last_dt, MAX))
        ch_consume += 100
        logger.info("URL:"+ch_response.url)
        
        if ch_response.status_code != 200:
            logger.error(ch_response.text)
            logger.error("##########エラーで終わり############")
            flag = True
            break
        ch_result = ch_response.json()
        logger.info(ch_result)
        ch_infos.extend([
            [item['id']['videoId'], item['snippet']['title'],'', item['snippet']['publishedAt']]
            for item in ch_result['items'] if item['id']['kind'] == 'youtube#video'])
        logger.info("取得したデータをch_infosに格納")

        if 'nextPageToken' in ch_result.keys():
            if 'pageToken' in ch_url:
                ch_url = ch_url.split('&pageToken')[0]
            ch_url += f'&pageToken={ch_result["nextPageToken"]}'
            logger.info('次のページに移ります。')
        else:
            print('ID.csvの更新を正常終了')
            break

    if not flag:
        news = pd.DataFrame(ch_infos, columns=['videoId', 'title','description', 'publishedAt']).sort_values('publishedAt', ascending=False)
        logger.info('新データを作り、日付順に並び替えました。')
        description_consume = get_descriptions(middle, last_dt, news, API_KEY)
        logger.info('最新のデータの概要欄を取得しました。')
        videos = news.append(videos)
        logger.info('新旧のチャンネルCSVを結合しました。')
        videos.to_csv( ch_name + '\\' + CHANNEL_ID + '.csv',encoding='utf-8_sig', index=None)
        logger.info('最新チャンネルCSVを出力しました。')
    else:
        pass

    return flag, ch_consume+description_consume

def fetch_vd_data(ch, start_dt, last_dt, ch_name, num, api): #ch = pd.read_csv(チャンネルID.csv')
    
    vd_url = base_url + '/videos?key=%s&id=%s&fields=items(id,contentDetails(duration),statistics)&part=id,contentDetails,statistics'
    vd_infos = []
    flag = False
    API_KEY = api
    vd_consume = 0
    logger.info("fetch_vd_data:初期定義:API,CHANNEL_ID,DATES,URL完了")

    for dt in ch['publishedAt']:
        
        #start_dt< ch['publishedAt'] < last_dt　の投稿日の動画を検索する        
        logger.info('次の動画IDが範囲内か確認します')
        if not(start_dt <= datetime.datetime.fromisoformat(dt.replace('Z', '')) <= last_dt):
            logger.info('範囲外や！')
            break

        if num <= 0:
            logger.info('設定した件数を取得したので終了します。')
            break

        q = ('publishedAt == "%s"' % dt)
        i = ch.query(q).index[0]
        VD_ID = ch.iloc[i][0]
        logger.info('投稿日から動画IDを取得しました。')
        
        while True:
            vd_response = requests.get(vd_url % (API_KEY, VD_ID))
            vd_consume += 5
            logger.debug(vd_response.url)
            if vd_response.status_code != 200:
                logger.error(vd_response.text)
                logger.error("##########エラーで終わり############")
                frag = True
                break
            vd_result = vd_response.json()

            logger.info('情報を格納します。')
            for item in vd_result['items']:
                tmp= []
                tmp.append(ch.iloc[i][0]) #ビデオID
                tmp.append(ch.iloc[i][1]) #タイトル
                tmp.append("")            #概要欄(description)
                tmp.append(ch.iloc[i][3]) #投稿日
                tmp.append(item['contentDetails']['duration'])
                tmp.append(item['statistics']['viewCount'])

                for i in ["likeCount","dislikeCount","favoriteCount","commentCount"]:
                    if i in item["statistics"]:
                        tmp.append(item['statistics'][i])
                    else:
                        tmp.append("None")

                # if 'likeCount' in item['statistics']:
                #     tmp.append(item['statistics']['likeCount'])
                # else:
                #     tmp.append("None")

                # if 'dislikeCount' in item['statistics']:
                #     tmp.append(item['statistics']['dislikeCount'])
                # else:
                #     tmp.append("None")
                
                # if 'favoriteCount' in item['statistics']:
                #     tmp.append(item['statistics']['favoriteCount'])
                # else:
                #     tmp.append("None")
                
                # if 'commentCount' in item['statistics']:
                #     tmp.append(item['statistics']['commentCount'])
                # else:
                #     tmp.append("None")

                logger.info(tmp)
                vd_infos.append(tmp)#２次元リスト作成
            logger.info('正常終了')
            num -= 1
            break

        if flag == True:
            break
    
    
    videos = pd.DataFrame(vd_infos, columns=['videoId','title', 'description', 'publishedAt', 'duration', 'viewCount', 'likeCount', 'dislikeCountv', 'favoriteCount', 'commentCount'])
    date = " "+str(datetime.datetime.now().date())
    time = " "+str(datetime.datetime.now().hour)
    videos.to_csv(ch_name + '\\' + ch_name + date + time + 'h.csv', encoding='utf-8_sig', index=None)
    logger.info('今回のビデオの日付.csvを作成しました')
    return vd_consume

def get_descriptions(start_dt, last_dt, news, api):
    vd_url = base_url + '/videos?key=%s&id=%s&fields=items(id,snippet(description))&part=id,snippet'
    start_dt = datetime.datetime.fromisoformat(start_dt.replace('Z', ''))
    last_dt = datetime.datetime.fromisoformat(last_dt.replace('Z', ''))
    API_KEY = api
    description_consume = 0
    logger.info("get_description初期定義:API,CHANNEL_ID,DATES,URL完了")

    for dt in news['publishedAt']:#news=新しい動画

        q = ('publishedAt == "%s"' % dt)
        i = news.query(q).index[0] #L202でも使う
        VD_ID = news.iloc[i][0]
        logger.info('投稿日から動画IDを検索しました。')
        
        while True:
            vd_response = requests.get(vd_url % (API_KEY, VD_ID))
            description_consume += 3
            logger.debug(vd_response.url)

            if vd_response.status_code != 200:
                logger.error(vd_response.text)
                logger.error("##########エラーで終わり############")
                break
            
            vd_result = vd_response.json()
            for item in vd_result['items']:
                news.iloc[i][2]=item['snippet']['description']#i=L186で定義
                logger.info('新動画のdescriptionをデータnewsに書き込みました。')
            break
        logger.info('ある動画の概要欄取得が正常終了しました。')

    return description_consume

def activate_log():
    today = str(datetime.date.today())
    logger = getLogger("logger")
    logger.setLevel(logging.INFO)
    sh = StreamHandler()
    sh.setLevel(logging.DEBUG)
    hf = Formatter('%(asctime)s - %(module)s - %(funcName)s - L%(lineno)d  - %(levelname)s - %(message)s')
    sh.setFormatter(hf)
    logger.addHandler(sh)

    fh = FileHandler('logs/' + today + '.log')
    fh.setLevel(logging.INFO)
    fh.setFormatter(hf)
    logger.addHandler(fh)
