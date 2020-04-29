
const puppeteer = require('puppeteer');
require('date-utils');
const {PubSub} = require('@google-cloud/pubsub');
let page;
var project_name= process.env.GCP_PROJECT;
var race_list = []
const pubSubClient = new PubSub(project_name);


async function publishMessage(data) {
  var topic_name = process.env.TOPIC_NAME;
  const dataBuffer = Buffer.from(JSON.stringify(data));
  const messageId = await pubSubClient.topic(topic_name).publish(dataBuffer);
  console.log(`Message ${messageId} published.::${data}`);

  } 
//ページを取得
async function getBrowserPage() {
  // Launch headless Chrome. Turn off sandbox so Chrome can run under root.
  const browser = await puppeteer.launch({ args: ['--no-sandbox'] });
  return browser.newPage();
}

exports.week_race_get = async (req, res) => {
  if (!page) {
    page = await getBrowserPage();
  }
　await page.goto('https://race.netkeiba.com/top/race_list.html')
  //実行時刻の取得
  const dateTime1 = new Date(); 
  //URL取得のための正規表現pattern
  const pattern = /shutuba.html/g;
  // ミリ秒にして9時間（32,400,000ミリ秒）足す
  dateTime1.setTime(dateTime1.getTime() + 1000 * 60 * 60 * 9);
  const click_date = dateTime1.toFormat('YYYYMMDD');
  //レース一覧
  await page.waitForXPath('//ul[@role = "tablist"]/li');
  const elementHandleList = await page.$x('//ul[@role = "tablist"]/li');

  for (const div of elementHandleList) {
    //レースリストのliタグ日付を取得(date)
    let date_set = await div.$x('@date');
    //レースリストのliタグcontrols取得(aria-controls)
    let id_set   = await div.$x('@aria-controls');
    //上記の日付とcontrolsの値抽出
    const date = (await (await date_set[0].getProperty('value')).jsonValue())
    const id   = (await (await id_set[0].getProperty('value')).jsonValue())
    //対象日付のdivタグをクリック
    if (date == click_date){
            await div.click()
            let listSelector="div#"+id + "> div > div > dl.RaceList_DataList";
            //クリック後に表示されたレースのURLを取得
            await page.waitForSelector(listSelector);
            const table_race = await page.$$(listSelector);
            for(const race of table_race){
                const r = await race.$$('a');
                for(const l of r){
                    const url = await (await l.getProperty('href')).jsonValue();
                    if ( url.match(/shutuba.html/)) {
                        const pub_url = url.replace( 'shutuba', 'shutuba_past' );
                        race_list.push({race_url:pub_url})
                    }
                }
            } 
            
        }

    }
    //publishを実行
    publishMessage(race_list).catch(console.error);
    res.send("Done");
};
