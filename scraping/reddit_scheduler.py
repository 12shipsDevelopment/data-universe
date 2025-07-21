import redis
import simplejson as json
from datetime import datetime, timedelta
from common.data import TimeBucket, DataSource

nsfw_labels = ['r/bimarriedmen', 'r/bigtitsinbikinis', 'r/phr4r_2', 'r/millennials_gone_wild', 'r/ratemynudebody', 'r/dadwouldbeproud', 'r/roleplaychatnsfw', 'r/hypnohookup', 'r/soccermomsgw', 'r/ladyboyporn', 'r/cutelittletits', 'r/cuckoldstories2', 'r/askdickpic', 'r/drogen', 'r/forcedfeminization', 'r/cheatingwives', 'r/futaroleplaypalace', 'r/wifebutt', 'r/coloradosex', 'r/breedmedaddy', 'r/asshole', 'r/femboyhentai', 'r/ebonyhomemade', 'r/twentyplus', 'r/sexybutclothed', 'r/sellingonline', 'r/nsfw_roleplay', 'r/oregonhookups2', 'r/cockcompareing', 'r/hungfemboys', 'r/wifewantstoplay', 'r/fuckingfascists', 'r/femdomcommunity', 'r/pussyperfectionx', 'r/pornstarhq', 'r/argnsfw', 'r/churchwife', 'r/18f', 'r/phatassfemboys', 'r/40_to_50_gone_wild', 'r/r4rseattle', 'r/facesittinghub', 'r/onlyfansfaces', 'r/bwc4bbw', 'r/perfectbody', 'r/breeding_her', 'r/dallasfreakz', 'r/blackchickswhitedicks', 'r/bicuriousgaychat', 'r/asiansgonewild', 'r/teendicks_', 'r/onlyfans101', 'r/smallcutie', 'r/boypussy', 'r/tinycuteteen', 'r/legalteens', 'r/onlyfansasstastic', 'r/pawglove', 'r/seduction', 'r/gaybbc', 'r/randomactsofblowjob', 'r/orlandor4r', 'r/onlinesugar', 'r/dolcett_fantasy', 'r/hotmoms', 'r/breedingbbw', 'r/onlyfans101inked', 'r/ratemyass_', 'r/onlyfans101supersized', 'r/blackgirlscentral', 'r/onlyfans101brandnew', 'r/mumbai_gw', 'r/cigars', 'r/mistresszone', 'r/miso_paradise', 'r/sissificationproject', 'r/emogirlsfuck', 'r/indiansissies', 'r/phonesex', 'r/theartofthetease', 'r/asiangirls4whitecocks', 'r/nofansallowed', 'r/hotdommes', 'r/girlscontrolled', 'r/mommy_tits', 'r/submissiveasiansluts', 'r/femboybussy', 'r/theadamfriedlandshow', 'r/obsf', 'r/degradethisthot', 'r/cougarsforcubs', 'r/asianhotties', 'r/a_cups', 'r/snappsexting', 'r/petite', 'r/goonedmeetup', 'r/bisexualfantasy', 'r/teengirlslover', 'r/b_cups', 'r/gayuklads', 'r/booty', 'r/biggerthanyourbfs', 'r/naughtywives', 'r/boobpicrequests', 'r/womenwholovebbc', 'r/onlyfans101bodymods', 'r/fat_fetish', 'r/askredditafterdark', 'r/dfwhotwife', 'r/blowjob', 'r/dilfs', 'r/bangladeshgonesexy', 'r/marvelrivalsr34nsfw', 'r/phgwcouples', 'r/bigdickgirl', 'r/universitygirls', 'r/foot_island', 'r/cdstoriesgonewild', 'r/promoteonlyfans', 'r/amifuckworthy', 'r/wankbattlesready', 'r/tipofmypenis', 'r/bulges', 'r/adorable_nudes', 'r/askredditnsfw', 'r/gaybrosgonewild', 'r/pantyhose', 'r/marriedbidownlow', 'r/ytnsfw', 'r/pussyaddicts', 'r/sapphicsexualityplay', 'r/naughtygrandma', 'r/masturbation', 'r/brisbanensfw', 'r/monsterfucker', 'r/pornrelapsed', 'r/gaygermany', 'r/sexsells', 'r/tiktoknsfw', 'r/sexystories', 'r/futarp', 'r/nsfwgenuniebeauties', 'r/janitorai_official', 'r/medical', 'r/gonewildtrans', 'r/slut', 'r/transformationrp', 'r/downblouse', 'r/bnwobsessed', 'r/homemadensfw', 'r/tributeme', 'r/lovebbws', 'r/onlyfansnaturallyhot', 'r/mindcontrolstories', 'r/ofgirlsselfies', 'r/narcoclips', 'r/gaybrosgonemild', 'r/gooned', 'r/barebody', 'r/onlyfansshmilfs', 'r/dirtyr4r', 'r/wife_wants_to_be_seen', 'r/nsfwchatsforfun', 'r/clubmilfs', 'r/totalbabes', 'r/eroticauthors', 'r/roleplay__hentai', 'r/ratemynudeselfie1', 'r/femaleorgasmdenial', 'r/dommes', 'r/literotica', 'r/thickthighs', 'r/milfbody', 'r/gaychats', 'r/asiangirlsforwhitemen', 'r/preguntasreddit_extra', 'r/dirtypenpals', 'r/traps', 'r/newkarmansfw18', 'r/onlyfans101bustybabes', 'r/jav', 'r/perfecttits', 'r/boltedontits', 'r/trapsarentgay', 'r/furryfemboy', 'r/full_nsfw', 'r/slutsofsnapchat', 'r/vaping', 'r/jerkoffchat', 'r/bigdickwhitedudes', 'r/girlswearingstrapons', 'r/pets_and_ownwers', 'r/phgonewildcurvy', 'r/nofans', 'r/agegap', 'r/ssbbw_fans', 'r/bigblackcocks', 'r/averagewife', 'r/iwanttobeherhentai2', 'r/ratemycock', 'r/gaychubs', 'r/hugeboobsandtits', 'r/massivecock', 'r/cuckold_nsfw_', 'r/shemale_big_cock', 'r/incesttabooporn', 'r/abdlstories', 'r/edgetogether', 'r/cougars_and_milfs_sfw', 'r/fanslynewbies', 'r/teenmassivecock', 'r/needysluts', 'r/hentaiandroleplayy', 'r/jerkinginstruction', 'r/onlyfansnevernude', 'r/bbc_sissycaptions', 'r/onlyfanssmallgirls', 'r/eroticliterature', 'r/moms_in_thongs', 'r/fuckmywife', 'r/tattooedgirls', 'r/bdsmcommunity', 'r/cnc_connect', 'r/bigasses', 'r/free_nudes_4_real', 'r/girlsmasturbating', 'r/sexting4hotgirlys', 'r/pornid', 'r/thickwhitegirls', 'r/long_porn', 'r/indianhotwife', 'r/nudes', 'r/daughtertraining', 'r/dirtychatpals', 'r/femdompersonals', 'r/cuckoldpsychology', 'r/edmontonr4r', 'r/bigtiddygothgf', 'r/findom', 'r/randomactsofmuffdive', 'r/indianroleplay', 'r/booty_lovers', 'r/corruptionhentai', 'r/aipornhub', 'r/sexover30', 'r/free_nude_karma', 'r/dykesgonewild', 'r/nederlandsegeilheid', 'r/greatview', 'r/telegram_slutty', 'r/pussy_perfection', 'r/dirtysnapchat', 'r/happy_nsfw', 'r/elitetransporn', 'r/argentinaporno', 'r/bdsmerotica', 'r/femcock', 'r/onlineaffairs', 'r/gonewildcd', 'r/twinks', 'r/wouldyoufuckmywife', 'r/atlantar4r', 'r/gentlefemdom', 'r/borntobefucked', 'r/pussy', 'r/bangaloregw', 'r/michigannaughtypeople', 'r/gonewildstories', 'r/boobs', 'r/straightturnedgay', 'r/mexicana', 'r/godpussy', 'r/18_22', 'r/onlyfans101badbitches', 'r/jerkbudshentai', 'r/adultcontentcreators', 'r/influencernsfw', 'r/onlyfanspetite', 'r/rate_my_feet', 'r/abdl', 'r/wife_gone_wild', 'r/sexstories', 'r/maconha', 'r/bangaloregwild', 'r/agegappersonals', 'r/notgayatall', 'r/onlyfans101tallgirls', 'r/lustforsex', 'r/chavsgalore', 'r/coffeegonewild', 'r/bronlyfans', 'r/onlyfansfootlovers', 'r/gymgirlsnsfw', 'r/sexualidade', 'r/onlyfans101fitgirls', 'r/punkgirls', 'r/blacked', 'r/gaystories', 'r/onlyfans101asstastic', 'r/gothwhoress', 'r/saggytit', 'r/broslikeus', 'r/normalnudesgonewild', 'r/youngslutsforoldpervs', 'r/1950shouseholdwives', 'r/chastitystories', 'r/onlyfans101hotmombods', 'r/bbw', 'r/18above_roleplay', 'r/bwc', 'r/pussypicrequest', 'r/karmansfw18', 'r/penis', 'r/barelylegalteens', 'r/assholegw', 'r/eroticwriting', 'r/bbcaddicts', 'r/narcissisticabuse', 'r/dfwcasualencounters', 'r/feminization', 'r/femboys4real', 'r/teencocksnew', 'r/bostonr4r', 'r/bbw4bwc', 'r/youngpussylips', 'r/solomasturbation', 'r/dirtyredditchat', 'r/gaysnapchatshare', 'r/snapchatbuds', 'r/ratemyboobs', 'r/gaybears', 'r/amihot', 'r/egirls', 'r/cglpersonals', 'r/nordgw', 'r/sexworkers', 'r/adorablenudes', 'r/momsonincest', 'r/nextdoorasians', 'r/gaystrugglefuck', 'r/askwomenadvice', 'r/hyperrp', 'r/gettingbigger', 'r/chubbydudes', 'r/chastitycouples', 'r/latinas', 'r/jerkofftoanime', 'r/nsfwiama', 'r/sailormoon', 'r/homewreckergirls', 'r/girlsgw', 'r/alasjuicy', 'r/nsfwbuys', 'r/milfs', 'r/femboys', 'r/findomforlife', 'r/largemilkers', 'r/piercednsfw', 'r/cameltoeoriginals', 'r/hentai', 'r/onlyifshespackin', 'r/gwasapphic', 'r/allporn4u', 'r/asianfetish', 'r/fresh_teendick', 'r/marriedandflirting', 'r/vaporents', 'r/hotwifelifestyle', 'r/sissypersonals', 'r/virginiagonewild', 'r/leagueoflegends', 'r/r4rmontreal', 'r/esposashotwife', 'r/bbcparadise', 'r/naturaltitties', 'r/kikroleplay', 'r/dirtyconfession', 'r/gayrp', 'r/fitnakedgirls', 'r/fakecartridges', 'r/dirtyukr4r', 'r/hairypussy', 'r/jumalattaret', 'r/sissyology', 'r/femboyhookup', 'r/collegesluts', 'r/fertilegirls', 'r/fastsexting', 'r/xsmallgirls', 'r/desistree', 'r/awesometransgirls', 'r/bustyqueens', 'r/maraikesallyear', 'r/sissykik2', 'r/buttsandbarefeet', 'r/kappachino', 'r/thicksloppycreamy', 'r/curvybutt', 'r/teentitans', 'r/femaleinferioritycap', 'r/onlyfans101bdsm', 'r/petitegonewild', 'r/kinktown', 'r/gonewild30plus', 'r/erotichypnosis', 'r/onlyfans101shorties', 'r/teensluttybodies', 'r/monstercocks', 'r/short_porn', 'r/taboo_rp', 'r/blackmailers', 'r/busty_girls', 'r/gaystoriesgonewild', 'r/transporn', 'r/nudegermans', 'r/nudenonnude', 'r/onlyfansblonde', 'r/nude_selfie', 'r/cock', 'r/huge_udders', 'r/findomgoddesses', 'r/dadsandboys', 'r/50_60plus_milfs', 'r/tightywhities', 'r/asstastic', 'r/topsandbottoms', 'r/ftmspunished', 'r/adultneeds', 'r/goonettehub', 'r/bbwpussys', 'r/cockheadlovers', 'r/chastity', 'r/tspetite', 'r/normalnudes', 'r/gaysnapchatimages', 'r/ftmporn', 'r/buttholespokes', 'r/nudegirlshub', 'r/pregnantgonewild', 'r/beast_love', 'r/indiansexting', 'r/sissychastity', 'r/sexyover50', 'r/verifiedfeet', 'r/milf', 'r/scotlandr4r', 'r/realgirls', 'r/feetinyourface', 'r/onlyfansfashionistas', 'r/tittydrop', 'r/brownhotties', 'r/rape_hentai', 'r/sexstoriesgonewild', 'r/50plusgw', 'r/bbws4bbcs', 'r/workgonewild', 'r/bestofrapefantasies', 'r/sex_treffen_germany', 'r/bicuriouswoman', 'r/cumsluts', 'r/womensupportsmisogyny', 'r/fuckinglikecrazy', 'r/ladybonersgw', 'r/trapsgonewild', 'r/gonewildaudio', 'r/nj4nj', 'r/snapchatdirtyx', 'r/pussy_delicious', 'r/smallboobs', 'r/pornstarvspornstar', 'r/losangelespersonals', 'r/healsluts', 'r/vrchaterp', 'r/malesmasturbating', 'r/marriedsex', 'r/fantasyerp', 'r/baddragon', 'r/hornywivesnextdoor', 'r/gayskype', 'r/chubbywifepics', 'r/backview', 'r/furryrp', 'r/gonewild', 'r/innocentlynaughty', 'r/bdsmpersonals', 'r/edgingtalk', 'r/frenchgonewild', 'r/homewrecker101', 'r/redditorcum', 'r/18y', 'r/facials', 'r/sissy_humiliation', 'r/smalldickpositivity', 'r/fnafpornrp', 'r/findaleague', 'r/averagepenis', 'r/socalswingers', 'r/pornhwa', 'r/dadsgonewild', 'r/jabbaleia', 'r/real_wives_of_reddit', 'r/blackworldorder', 'r/assmasterpiece', 'r/sissies', 'r/puppygirlpetsmart', 'r/chubbywomen', 'r/gbr4r', 'r/straighttosissy', 'r/blondes', 'r/hugetitsndass', 'r/footfetishtalks', 'r/thickdick', 'r/gooningtrees', 'r/cheatingpov', 'r/mommyheaven', 'r/slutwife', 'r/swingersr4r', 'r/floridahookups', 'r/blacktittyworld', 'r/gayporn_nsfw', 'r/lovense', 'r/fapdeciders', 'r/nude1819', 'r/freekarmauncut', 'r/bbw_chubby', 'r/40plusgonewild', 'r/tgirlsporn', 'r/defeatedhentai', 'r/orc34', 'r/onlyfans101genz', 'r/onlyfansbusty', 'r/gothsluts', 'r/titpicsrequest', 'r/confessionsgonewild', 'r/churchofmen', 'r/hentairoleplay', 'r/armpitfetish', 'r/chubby', 'r/onlyfansbrunette', 'r/oldermanpersonals', 'r/onepiecehentaiz', 'r/cuckoldhumiliation', 'r/transgonewild', 'r/snapchatsellers_x', 'r/gaydadsandboys', 'r/daddysdarkfiction', 'r/gonewild18', 'r/gilf', 'r/cumflation_rp', 'r/gay_rape_', 'r/braless', 'r/daddy', 'r/onlyfansjustright', 'r/just18', 'r/snapchatsexting123', 'r/raceplay']

REDDIT_TASK_INDEX="reddit:label:task_index" # int
REDDIT_QUEUE_KEY="reddit:label:task_queue" # list
REDDIT_ADDED_KEY="reddit:label:task_added"  # set

def read_default_labels():
    try:
        with open("scraping/default_reddit_labels.json", "r") as f:
            labels = json.load(f)
            if isinstance(labels, list):
                return labels
    except FileNotFoundError:
        pass
    return []

class RedditScheduler:
    @classmethod
    def from_conn(cls, host: str, port: int, password: str):
        r = redis.Redis(host=host, port=port, db=0, password=password)
        return cls(r)

    def __init__(self, r):
        self.r = r
        self.labels = read_default_labels()
        print(f"load {len(self.labels)} labels from default_labels.json")
        self.total = []

    def get_task(self):
        script = """
            local queue_len = redis.call('LLEN', KEYS[1])
            local queue_index

            if redis.call('EXISTS', KEYS[2]) == 1 then
                queue_index = tonumber(redis.call('GET', KEYS[2]))
            else
                queue_index = 0
            end

            local current_index = queue_index  -- 保存当前值，用于返回

            if queue_index + 1 < queue_len then
                redis.call('SET', KEYS[2], queue_index + 1)
            else
                redis.call('SET', KEYS[2], 0)
            end

            return current_index  -- 返回自增前的值
            """

        current_index = self.r.eval(script, 2, REDDIT_QUEUE_KEY, REDDIT_TASK_INDEX)
        print(current_index)  # 输出自增前的 queue_index

        
        task_data = self.r.lindex(REDDIT_QUEUE_KEY, current_index)
        
        if not task_data:
            print("LabelScheduler: No new tasks, waiting...")
            return None, 0

        task = json.loads(task_data)
        return task, current_index

    '''
    scraper放回来的时候应该left=False
    '''
    def update_task(self, task, index):
        
        task_data = json.dumps(task)
        self.r.lset(REDDIT_QUEUE_KEY, index, task_data)
        
    def add_task(self, task):
        added = self.r.sismember(REDDIT_ADDED_KEY, task["label"])
        if not added:
            self.r.rpush(REDDIT_QUEUE_KEY, json.dumps(task))
            self.r.sadd(REDDIT_ADDED_KEY,task["label"])
            print("LabelScheduler: Added new task: ", task)

    def init_tasks(self, labels = None):
        now = datetime.now()
        time = now - timedelta(hours = 36, minutes= 10)
        timestamp = int(time.timestamp())
        if not labels:
            for label in nsfw_labels:
                if not self.r.sismember(REDDIT_ADDED_KEY, label):
                    self.add_task({
                        "label": label,
                        "post_before": timestamp,
                        "post_after": timestamp,
                        "post_latest": 0,
                        "comment_before": timestamp,
                        "comment_after": timestamp,
                        "comment_latest": 0,
                        "is_nsfw": True
                    })
            labels= self.labels
        
        for label in labels:
            if not self.r.sismember(REDDIT_ADDED_KEY, label):
                self.add_task({
                    "label": label,
                    "post_before": timestamp,
                    "post_after": timestamp,
                    "post_latest": 0,
                    "comment_before": timestamp,
                    "comment_after": timestamp,
                    "comment_latest": 0,
                    "is_nsfw": False
                })
                
