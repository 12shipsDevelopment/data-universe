import redis
import simplejson as json
from datetime import datetime, timedelta
from common.data import TimeBucket, DataSource

nsfw_labels = ['r/bimarriedmen', 'r/bigtitsinbikinis', 'r/phr4r_2', 'r/millennials_gone_wild', 'r/ratemynudebody', 'r/dadwouldbeproud', 'r/roleplaychatnsfw', 'r/hypnohookup', 'r/soccermomsgw', 'r/ladyboyporn', 'r/cutelittletits', 'r/cuckoldstories2', 'r/askdickpic', 'r/drogen', 'r/forcedfeminization', 'r/cheatingwives', 'r/futaroleplaypalace', 'r/wifebutt', 'r/coloradosex', 'r/breedmedaddy', 'r/asshole', 'r/femboyhentai', 'r/ebonyhomemade', 'r/twentyplus', 'r/sexybutclothed', 'r/sellingonline', 'r/nsfw_roleplay', 'r/oregonhookups2', 'r/cockcompareing', 'r/hungfemboys', 'r/wifewantstoplay', 'r/fuckingfascists', 'r/femdomcommunity', 'r/pussyperfectionx', 'r/pornstarhq', 'r/argnsfw', 'r/churchwife', 'r/18f', 'r/phatassfemboys', 'r/40_to_50_gone_wild', 'r/r4rseattle', 'r/facesittinghub', 'r/onlyfansfaces', 'r/bwc4bbw', 'r/perfectbody', 'r/breeding_her', 'r/dallasfreakz', 'r/blackchickswhitedicks', 'r/bicuriousgaychat', 'r/asiansgonewild', 'r/teendicks_', 'r/onlyfans101', 'r/smallcutie', 'r/boypussy', 'r/tinycuteteen', 'r/legalteens', 'r/onlyfansasstastic', 'r/pawglove', 'r/seduction', 'r/gaybbc', 'r/randomactsofblowjob', 'r/orlandor4r', 'r/onlinesugar', 'r/dolcett_fantasy', 'r/hotmoms', 'r/breedingbbw', 'r/onlyfans101inked', 'r/ratemyass_', 'r/onlyfans101supersized', 'r/blackgirlscentral', 'r/onlyfans101brandnew', 'r/mumbai_gw', 'r/cigars', 'r/mistresszone', 'r/miso_paradise', 'r/sissificationproject', 'r/emogirlsfuck', 'r/indiansissies', 'r/phonesex', 'r/theartofthetease', 'r/asiangirls4whitecocks', 'r/nofansallowed', 'r/hotdommes', 'r/girlscontrolled', 'r/mommy_tits', 'r/submissiveasiansluts', 'r/femboybussy', 'r/theadamfriedlandshow', 'r/obsf', 'r/degradethisthot', 'r/cougarsforcubs', 'r/asianhotties', 'r/a_cups', 'r/snappsexting', 'r/petite', 'r/goonedmeetup', 'r/bisexualfantasy', 'r/teengirlslover', 'r/b_cups', 'r/gayuklads', 'r/booty', 'r/biggerthanyourbfs', 'r/naughtywives', 'r/boobpicrequests', 'r/womenwholovebbc', 'r/onlyfans101bodymods', 'r/fat_fetish', 'r/askredditafterdark', 'r/dfwhotwife', 'r/blowjob', 'r/dilfs', 'r/bangladeshgonesexy', 'r/marvelrivalsr34nsfw', 'r/phgwcouples', 'r/bigdickgirl', 'r/universitygirls', 'r/foot_island', 'r/cdstoriesgonewild', 'r/promoteonlyfans', 'r/amifuckworthy', 'r/wankbattlesready', 'r/tipofmypenis', 'r/bulges', 'r/adorable_nudes', 'r/askredditnsfw', 'r/gaybrosgonewild', 'r/pantyhose', 'r/marriedbidownlow', 'r/ytnsfw', 'r/pussyaddicts', 'r/sapphicsexualityplay', 'r/naughtygrandma', 'r/masturbation', 'r/brisbanensfw', 'r/monsterfucker', 'r/pornrelapsed', 'r/gaygermany', 'r/sexsells', 'r/tiktoknsfw', 'r/sexystories', 'r/futarp', 'r/nsfwgenuniebeauties', 'r/janitorai_official', 'r/medical', 'r/gonewildtrans', 'r/slut', 'r/transformationrp', 'r/downblouse', 'r/bnwobsessed', 'r/homemadensfw', 'r/tributeme', 'r/lovebbws', 'r/onlyfansnaturallyhot', 'r/mindcontrolstories', 'r/ofgirlsselfies', 'r/narcoclips', 'r/gaybrosgonemild', 'r/gooned', 'r/barebody', 'r/onlyfansshmilfs', 'r/dirtyr4r', 'r/wife_wants_to_be_seen', 'r/nsfwchatsforfun', 'r/clubmilfs', 'r/totalbabes', 'r/eroticauthors', 'r/roleplay__hentai', 'r/ratemynudeselfie1', 'r/femaleorgasmdenial', 'r/dommes', 'r/literotica', 'r/thickthighs', 'r/milfbody', 'r/gaychats', 'r/asiangirlsforwhitemen', 'r/preguntasreddit_extra', 'r/dirtypenpals', 'r/traps', 'r/newkarmansfw18', 'r/onlyfans101bustybabes', 'r/jav', 'r/perfecttits', 'r/boltedontits', 'r/trapsarentgay', 'r/furryfemboy', 'r/full_nsfw', 'r/slutsofsnapchat', 'r/vaping', 'r/jerkoffchat', 'r/bigdickwhitedudes', 'r/girlswearingstrapons', 'r/pets_and_ownwers', 'r/phgonewildcurvy', 'r/nofans', 'r/agegap', 'r/ssbbw_fans', 'r/bigblackcocks', 'r/averagewife', 'r/iwanttobeherhentai2', 'r/ratemycock', 'r/gaychubs', 'r/hugeboobsandtits', 'r/massivecock', 'r/cuckold_nsfw_', 'r/shemale_big_cock', 'r/incesttabooporn', 'r/abdlstories', 'r/edgetogether', 'r/cougars_and_milfs_sfw', 'r/fanslynewbies', 'r/teenmassivecock', 'r/needysluts', 'r/hentaiandroleplayy', 'r/jerkinginstruction', 'r/onlyfansnevernude', 'r/bbc_sissycaptions', 'r/onlyfanssmallgirls', 'r/eroticliterature', 'r/moms_in_thongs', 'r/fuckmywife', 'r/tattooedgirls', 'r/bdsmcommunity', 'r/cnc_connect', 'r/bigasses', 'r/free_nudes_4_real', 'r/girlsmasturbating', 'r/sexting4hotgirlys', 'r/pornid', 'r/thickwhitegirls', 'r/long_porn', 'r/indianhotwife', 'r/nudes', 'r/daughtertraining', 'r/dirtychatpals', 'r/femdompersonals', 'r/cuckoldpsychology', 'r/edmontonr4r', 'r/bigtiddygothgf', 'r/findom', 'r/randomactsofmuffdive', 'r/indianroleplay', 'r/booty_lovers', 'r/corruptionhentai', 'r/aipornhub', 'r/sexover30', 'r/free_nude_karma', 'r/dykesgonewild', 'r/nederlandsegeilheid', 'r/greatview', 'r/telegram_slutty', 'r/pussy_perfection', 'r/dirtysnapchat', 'r/happy_nsfw', 'r/elitetransporn', 'r/argentinaporno', 'r/bdsmerotica', 'r/femcock', 'r/onlineaffairs', 'r/gonewildcd', 'r/twinks', 'r/wouldyoufuckmywife', 'r/atlantar4r', 'r/gentlefemdom', 'r/borntobefucked', 'r/pussy', 'r/bangaloregw', 'r/michigannaughtypeople', 'r/gonewildstories', 'r/boobs', 'r/straightturnedgay', 'r/mexicana', 'r/godpussy', 'r/18_22', 'r/onlyfans101badbitches', 'r/jerkbudshentai', 'r/adultcontentcreators', 'r/influencernsfw', 'r/onlyfanspetite', 'r/rate_my_feet', 'r/abdl', 'r/wife_gone_wild', 'r/sexstories', 'r/maconha', 'r/bangaloregwild', 'r/agegappersonals', 'r/notgayatall', 'r/onlyfans101tallgirls', 'r/lustforsex', 'r/chavsgalore', 'r/coffeegonewild', 'r/bronlyfans', 'r/onlyfansfootlovers', 'r/gymgirlsnsfw', 'r/sexualidade', 'r/onlyfans101fitgirls', 'r/punkgirls', 'r/blacked', 'r/gaystories', 'r/onlyfans101asstastic', 'r/gothwhoress', 'r/saggytit', 'r/broslikeus', 'r/normalnudesgonewild', 'r/youngslutsforoldpervs', 'r/1950shouseholdwives', 'r/chastitystories', 'r/onlyfans101hotmombods', 'r/bbw', 'r/18above_roleplay', 'r/bwc', 'r/pussypicrequest', 'r/karmansfw18', 'r/penis', 'r/barelylegalteens', 'r/assholegw', 'r/eroticwriting', 'r/bbcaddicts', 'r/narcissisticabuse', 'r/dfwcasualencounters', 'r/feminization', 'r/femboys4real', 'r/teencocksnew', 'r/bostonr4r', 'r/bbw4bwc', 'r/youngpussylips', 'r/solomasturbation', 'r/dirtyredditchat', 'r/gaysnapchatshare', 'r/snapchatbuds', 'r/ratemyboobs', 'r/gaybears', 'r/amihot', 'r/egirls', 'r/cglpersonals', 'r/nordgw', 'r/sexworkers', 'r/adorablenudes', 'r/momsonincest', 'r/nextdoorasians', 'r/gaystrugglefuck', 'r/askwomenadvice', 'r/hyperrp', 'r/gettingbigger', 'r/chubbydudes', 'r/chastitycouples', 'r/latinas', 'r/jerkofftoanime', 'r/nsfwiama', 'r/sailormoon', 'r/homewreckergirls', 'r/girlsgw', 'r/alasjuicy', 'r/nsfwbuys', 'r/milfs', 'r/femboys', 'r/findomforlife', 'r/largemilkers', 'r/piercednsfw', 'r/cameltoeoriginals', 'r/hentai', 'r/onlyifshespackin', 'r/gwasapphic', 'r/allporn4u', 'r/asianfetish', 'r/fresh_teendick', 'r/marriedandflirting', 'r/vaporents', 'r/hotwifelifestyle', 'r/sissypersonals', 'r/virginiagonewild', 'r/leagueoflegends', 'r/r4rmontreal', 'r/esposashotwife', 'r/bbcparadise', 'r/naturaltitties', 'r/kikroleplay', 'r/dirtyconfession', 'r/gayrp', 'r/fitnakedgirls', 'r/fakecartridges', 'r/dirtyukr4r', 'r/hairypussy', 'r/jumalattaret', 'r/sissyology', 'r/femboyhookup', 'r/collegesluts', 'r/fertilegirls', 'r/fastsexting', 'r/xsmallgirls', 'r/desistree', 'r/awesometransgirls', 'r/bustyqueens', 'r/maraikesallyear', 'r/sissykik2', 'r/buttsandbarefeet', 'r/kappachino', 'r/thicksloppycreamy', 'r/curvybutt', 'r/teentitans', 'r/femaleinferioritycap', 'r/onlyfans101bdsm', 'r/petitegonewild', 'r/kinktown', 'r/gonewild30plus', 'r/erotichypnosis', 'r/onlyfans101shorties', 'r/teensluttybodies', 'r/monstercocks', 'r/short_porn', 'r/taboo_rp', 'r/blackmailers', 'r/busty_girls', 'r/gaystoriesgonewild', 'r/transporn', 'r/nudegermans', 'r/nudenonnude', 'r/onlyfansblonde', 'r/nude_selfie', 'r/cock', 'r/huge_udders', 'r/findomgoddesses', 'r/dadsandboys', 'r/50_60plus_milfs', 'r/tightywhities', 'r/asstastic', 'r/topsandbottoms', 'r/ftmspunished', 'r/adultneeds', 'r/goonettehub', 'r/bbwpussys', 'r/cockheadlovers', 'r/chastity', 'r/tspetite', 'r/normalnudes', 'r/gaysnapchatimages', 'r/ftmporn', 'r/buttholespokes', 'r/nudegirlshub', 'r/pregnantgonewild', 'r/beast_love', 'r/indiansexting', 'r/sissychastity', 'r/sexyover50', 'r/verifiedfeet', 'r/milf', 'r/scotlandr4r', 'r/realgirls', 'r/feetinyourface', 'r/onlyfansfashionistas', 'r/tittydrop', 'r/brownhotties', 'r/rape_hentai', 'r/sexstoriesgonewild', 'r/50plusgw', 'r/bbws4bbcs', 'r/workgonewild', 'r/bestofrapefantasies', 'r/sex_treffen_germany', 'r/bicuriouswoman', 'r/cumsluts', 'r/womensupportsmisogyny', 'r/fuckinglikecrazy', 'r/ladybonersgw', 'r/trapsgonewild', 'r/gonewildaudio', 'r/nj4nj', 'r/snapchatdirtyx', 'r/pussy_delicious', 'r/smallboobs', 'r/pornstarvspornstar', 'r/losangelespersonals', 'r/healsluts', 'r/vrchaterp', 'r/malesmasturbating', 'r/marriedsex', 'r/fantasyerp', 'r/baddragon', 'r/hornywivesnextdoor', 'r/gayskype', 'r/chubbywifepics', 'r/backview', 'r/furryrp', 'r/gonewild', 'r/innocentlynaughty', 'r/bdsmpersonals', 'r/edgingtalk', 'r/frenchgonewild', 'r/homewrecker101', 'r/redditorcum', 'r/18y', 'r/facials', 'r/sissy_humiliation', 'r/smalldickpositivity', 'r/fnafpornrp', 'r/findaleague', 'r/averagepenis', 'r/socalswingers', 'r/pornhwa', 'r/dadsgonewild', 'r/jabbaleia', 'r/real_wives_of_reddit', 'r/blackworldorder', 'r/assmasterpiece', 'r/sissies', 'r/puppygirlpetsmart', 'r/chubbywomen', 'r/gbr4r', 'r/straighttosissy', 'r/blondes', 'r/hugetitsndass', 'r/footfetishtalks', 'r/thickdick', 'r/gooningtrees', 'r/cheatingpov', 'r/mommyheaven', 'r/slutwife', 'r/swingersr4r', 'r/floridahookups', 'r/blacktittyworld', 'r/gayporn_nsfw', 'r/lovense', 'r/fapdeciders', 'r/nude1819', 'r/freekarmauncut', 'r/bbw_chubby', 'r/40plusgonewild', 'r/tgirlsporn', 'r/defeatedhentai', 'r/orc34', 'r/onlyfans101genz', 'r/onlyfansbusty', 'r/gothsluts', 'r/titpicsrequest', 'r/confessionsgonewild', 'r/churchofmen', 'r/hentairoleplay', 'r/armpitfetish', 'r/chubby', 'r/onlyfansbrunette', 'r/oldermanpersonals', 'r/onepiecehentaiz', 'r/cuckoldhumiliation', 'r/transgonewild', 'r/snapchatsellers_x', 'r/gaydadsandboys', 'r/daddysdarkfiction', 'r/gonewild18', 'r/gilf', 'r/cumflation_rp', 'r/gay_rape_', 'r/braless', 'r/daddy', 'r/onlyfansjustright', 'r/just18', 'r/snapchatsexting123', 'r/raceplay']

TASK_COMPLETED_KEY="x:label:task_completed" # set timeBucketId

X_QUEUE_KEY="x:label:task_queue" # list
X_ADDED_KEY="x:label:task_added"  # set

REDDIT_QUEUE_KEY="reddit:label:task_queue" # list
REDDIT_ADDED_KEY="reddit:label:task_added"  # set

def read_default_labels():
    try:
        with open("default_labels.json", "r") as f:
            labels = json.load(f)
            if isinstance(labels, list):
                return labels
    except FileNotFoundError:
        pass
    return []

class LabelScheduler:
    @classmethod
    def from_conn(cls, host: str, port: int, password: str):
        r = redis.Redis(host=host, port=port, db=0, password=password)
        return cls(r)

    def __init__(self, r):
        self.r = r
        self.labels = read_default_labels()
        print(f"load {len(self.labels)} labels from default_labels.json")
        self.total = []
        self.trends = []
        self.labels.reverse()

    def __key(self, label, bucketId):    
        return f"{bucketId}-{label}"

    def get_task(self, reddit_only = False):
        lua = """
        local task_data = redis.call('RPOP', KEYS[1])
        if not task_data then
            return nil
        end

        local task = cjson.decode(task_data)

        -- 使用 bucketId-label 格式生成 key
        local set_key = task.timeBucketId .. "-" .. task.label
        redis.call('SREM', KEYS[2], set_key)

        return task_data
        """
        if reddit_only:
            task_data = self.r.eval(lua, 2, REDDIT_QUEUE_KEY, REDDIT_ADDED_KEY)
        else:
            task_data = self.r.eval(lua, 2, X_QUEUE_KEY, X_ADDED_KEY)

        if not task_data:
            print("LabelScheduler: No new tasks, waiting...")
            return None

        task = json.loads(task_data)
        return task

    '''
    scraper放回来的时候应该left=False
    '''
    def add_task(self, task, left=True):
        lua = """
            local in_added = redis.call('SISMEMBER', KEYS[1], ARGV[2])
            local in_completed = redis.call('SISMEMBER', KEYS[2], ARGV[2])

            if in_added == 0 and in_completed == 0 then
                -- Add to queue based on left flag
                if ARGV[3] == "1" then
                    redis.call('LPUSH', KEYS[3], ARGV[1])
                else
                    redis.call('RPUSH', KEYS[3], ARGV[1])
                end
                -- Add to added set
                redis.call('SADD', KEYS[1], ARGV[2])
                return 1  -- Success
            end
            return 0  -- Not added
        """
        key = self.__key(task['label'], task['timeBucketId'])
        if task["source"] == DataSource.X:
            added = self.r.eval(lua, 3,
                    X_ADDED_KEY,
                    TASK_COMPLETED_KEY,
                    X_QUEUE_KEY,
                    json.dumps(task),
                    key,
                    "1" if left else "0")
        elif task["source"] == DataSource.REDDIT:
            added = self.r.eval(lua, 3,
                    REDDIT_ADDED_KEY,
                    TASK_COMPLETED_KEY,
                    REDDIT_QUEUE_KEY,
                    json.dumps(task),
                    key,
                    "1" if left else "0")

        if added:
            print("LabelScheduler: Added new task: ", task)

    def complete_task(self, label, timeBucketId):
        self.r.sadd(TASK_COMPLETED_KEY, self.__key(label, timeBucketId))

    def init_tasks(self, labels, days_back=30):
        now = datetime.now()
        start = now - timedelta(days=days_back)
        left_boundary = now - timedelta(days=10)
        left = True
        while start <= now:
            timeBucketId = TimeBucket.from_datetime(start).id - 1
            if left and start >= left_boundary:
                left = False
            for label in labels:
                if label.startswith('#'):
                    self.add_task({
                        "timeBucketId": timeBucketId,
                        "contentSizeBytes": 0,
                        "label": label,
                        "cursor": None,
                        "source": DataSource.X
                    }, left=left)
                elif label.startswith('r/'):
                    self.add_task({
                        "timeBucketId": timeBucketId,
                        "contentSizeBytes": 0,
                        "label": label,
                        "cursor": None,
                        "source": DataSource.REDDIT
                    }, left=left)
            start += timedelta(hours=1)
        print("LabelScheduler: initialize 30days tasks completed")

    def schedule_realtime_tasks(self):
        now = datetime.now()
        timeBucketId = TimeBucket.from_datetime(now).id - 1
        for label in self.labels + self.trends + self.total:
            if label.startswith('#'):
                self.add_task({
                    "timeBucketId": timeBucketId,
                    "contentSizeBytes": 0,
                    "label": label,
                    "cursor": None,
                    "source": DataSource.X
                }, left=False)
            elif label.startswith('r/'):
                self.add_task({
                    "timeBucketId": timeBucketId,
                    "contentSizeBytes": 0,
                    "label": label,
                    "cursor": None,
                    "source": DataSource.REDDIT
                }, left=False)
