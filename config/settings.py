from fuzzywuzzy import fuzz
import pyspark.sql.functions as F
import pandas as pd
import dlh_utils
import openpyxl
import xlrd
import re
import functools

from dlh_utils import utilities
from dlh_utils import dataframes
from dlh_utils import linkage
from dlh_utils import standardisation
from dlh_utils import sessions
from dlh_utils import profiling
from dlh_utils import flags
from pyspark.sql.functions import col, concat, lit
from pyspark.sql.functions import length
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, trim
from pyspark.sql.functions import regexp_replace, split, array_distinct, concat_ws
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_extract, col, trim, regexp_replace, when
from pyspark.sql.functions import count
from pyspark.sql.functions import sum as F_sum
from functools import reduce
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

from pyspark.sql import *
from pyspark.sql.functions import substring
from pyspark.sql.functions import when
from pyspark.sql.functions import udf
from functools import reduce
import re
from collections import Counter

##########################################################################
# making these universal variables for many of the functions used: 
# make alphabetical order
town_list = ["ABERDARE", "ABERFELDY", "ABERGELE", "ABERTAWE", "ABERTILLERY", "ABERYSTWYTH",
"ABINGDON", "ABINGDON-ON-THAMES", "ABOYNE", "ABRIDGE", "ACCRINGTON", "ADDERBURY",
"ADLINGTON", "AINTREE", "AIRDRIE", "AIRTH", "ALDBOURNE", "ALDBROUGH", "ALDERSHOT",
"ALDERLEY EDGE", "ALDERMASTON", "ALDINGTON", "ALDRIDGE", "ALLITHWAITE", "ALMONDBURY",
"ALNMOUTH", "ALRESFORD", "ALREWAS", "ALSAGER", "ALSTON", "ALVECHURCH",
"ALWINTON", "AMBLESIDE", "AMELIAH ISLAND", "AMERSHAM", "AMPTHILL", "ANDOVER", "ANGMERING", "ANSTEY",
"APPLEDOR", "ARBROOK", "ARDGAY", "ARDRISHAIG", "ARLESEY", "ARMADALE", "ARDROSSAN", 
"ARNISTON", "ARNSIDE", "ARUN", "ASH", "ASHBURTON", "ASHBRIDGE", "ASHFORD", "ASHBOURNE",
"ASHOVER", "ASHPERTON", "ASKRIGG", "ASPATRIA", "ASTWOOD BANK", "ATHERTON",
"AUDLEM", "AVEBURY", "AXBRIDGE", "AXMINSTER", "AYCLIFFE", "AYLSHAM", "AYLESBURY",
"AYNHO", "AYOT ST LAWRENCE", "AYRSHIRE", "BACKWELL", "BACTON", "BADINGHAM",
"BADMINTON", "BAGSHOT", "BALDOCK", "BALERNO", "BALLATER", "BALLOCH",
"BAMBURGH", "BAMPTON", "BANCHORY", "BANHAM", "BANKNOCK", "BARDSEA", "BARKHAM",
"BARKISLAND", "BARLASTON", "BARLBOROUGH", "BARLOW", "BARMOUTH", "BARNBY MOOR",
"BARNSLEY", "BARNSTON", "BARRINGTON", "BARROW", "BARROWDEN", "BARTON",
"BARTON ST DAVID", "BARWELL", "BASCHURCH", "BASLOW", "BASSINGHAM", "BATH", "BATHFORD",
"BATLEY", "BEACONSFIELD", "BEARSDEN", "BEAUMARIS", "BEBINGTON",
"BECCLES", "BEDALE", "BEDFORD", "BEDWORTH", "BEESTON", "BELFAST", "BELLINGHAM", "BELLSHILL",
"BELPER", "BERKHAMSTED", "BERWICK-UPON-TWEED", "BEVERLEY", "BEXHILL-ON-SEA", "BEXLEY", "BEXLEYHEATH", "BICESTER",
"BIDEFORD", "BIGGAR", "BIGGLESWADE", "BILLERICAY", "BILLINGHAM", "BILSTON", "BINGLEY", "BIRCHINGTON-ON-SEA",
"BIRKENHEAD", "BIRMINGHAM","BISHOP AUCKLAND", "BISHOP'S STORTFORD", "BISHOPBRIGGS", "BISHOPS CASTLE", "BLACKBURN", "BLACKHEATH", "BLACKPOOL",
"BLAENAU FFESTINIOG", "BLAIRGOWRIE", "BLANTYRE", "BLAYDON", "BLETCHLEY", "BLOXWICH", "BLYTH", "BO'NESS",
"BODMIN", "BOGNOR REGIS", "BOLLINGTON", "BOLSOVER", "BOLTON", "BONESS", "BOOTLE", "BOREHAM", "BOREHAMWOOD",
"BOROUGHBRIDGE", "BOSTON", "BOURNEMOUTH", "BRACKLEY", "BRACKNELL", "BRADFORD", "BRADFORD-ON-AVON", "BRADLEY STOKE",
"BRAINTREE", "BRAMHALL", "BRAMPTON", "BRECHIN", "BRECON", "BRENTFORD", "BRENTWOOD", "BRIDGEND",
"BRIDGNORTH", "BRIDGWATER", "BRIDLINGTON", "BRIDPORT", "BRIERLEY HILL", "BRIGG", "BRIGHOUSE", "BRIGHTLINGSEA",
"BRIGHTON AND HOVE", "BRIGHTON", "BRISTOL", "BROADSTAIRS", "BROMLEY", "BROMYARD", "BROMSGROVE", "BROXBURN", "BUCKIE", "BUCKINGHAM",
"BUCKLEY", "BUNGAY", "BURGESS HILL", "BURNLEY", "BURNTISLAND", "BURNTWOOD", "BURRY PORT", "BURTON LATIMER",
"BURTON UPON TRENT", "BURY", "BURY ST EDMUNDS", "BUSHEY", "BUXTON", "CAERNARFON", "CAERPHILLY", "CALDICOT", "CALLANDER", "CALLINGTON",
"CAMBERLEY", "CAMBORNE", "CAMBRIDGE", "CAMPBELTOWN", "CANNESBY", "CANNOCK", "CANVEY ISLAND", "CARDIFF",
"CARDIGAN", "CARLISLE", "CARLUKE", "CARMARTHEN", "CARNFORTH", "CARRICKFERGUS", "CARSHALTON", "CARTERTON", "CASTLETOWN",
"CASTLEFORD", "CASTLEWELLAN", "CATERHAM", "CHARD", "CHADDERTON", "CHARLTON KINGS", "CHATHAM", "CHEDDAR", "CHELMSFORD",
"CHELTENHAM", "CHEPSTOW", "CHERTSEY", "CHESTER","CHESHAM", "CHESHUNT", "CHESSINGTON", "CHESTER-LE-STREET", "CHESTERFIELD",
"CHICHESTER", "CHIGWELL", "CHIPPENHAM", "CHIPPING NORTON", "CHIPPING SODBURY", "CHORLEY", "CHRISTCHURCH", "CHURCH STRETTON", "CINDERFORD",
"CIRENCESTER", "CLACTON-ON-SEA", "CLECKHEATON", "CLEETHORPES", "CLEVEDON", "CLITHEROE", "COALVILLE", "COATBRIDGE",
"COBHAM", "COCKERMOUTH", "COLCHESTER", "COLDSTREAM", "COLWYN BAY", "CONGLETON", "CONSETT", "CORBY", "CORWEN", 
"COSHAM", "COTGRAVE", "COTTINGHAM", "COULSDON", "COVENTRY", "COWBRIDGE", "COWDENBEATH", "COWES",
"CRAIGAVON", "CRAMLINGTON", "CRAVEN ARMS",  "CRAWLEY", "CREWE", "CREDITON", "CREWKERNE", "CROMER", "CROWBOROUGH", "CROWLE",
"CROWTHORNE", "CROYDON", "CULLOMPTON", "CUMNOCK", "CUPAR", "CWMBRAN", "DALBEATTIE", "DALKEITH",
"DARLINGTON", "DARTFORD", "DARTMOUTH", "DARWEN", "DAVENTRY", "DAWLISH", "DEAL", "DENBIGH", "DENNY",
"DENTON", "DERBY", "DEREHAM", "DEVIZES", "DEWSBURY", "DIDCOT", "DINGWALL", "DINNINGTON",
"DISS", "DOLGELLAU", "DONAGHADEE", "DONCASTER", "DORCHESTER", "DORKING", "DOUGLAS", "DOVER", "DOWNHAM MARKET",
"DOWNPATRICK", "DRIFFIELD", "DROITWICH", "DROITWICH SPA", "DROMORE", "DROYLSDEN", "DUDLEY", "DUFFTOWN",
"DUMBARTON", "DUMFRIES", "DUNBAR", "DUNBLANE", "DUNDEE", "DUNFERMLINE", "DUNGANNON", "DUNOON",
"DUNS", "DUNSTABLE", "DURHAM", "DURSLEY", "EALING", "EARLS COLNE", "EAST GRINSTEAD", "EAST KILBRIDE",
"EASTBOURNE", "EASTLEIGH", "EASTWOOD", "EBBW VALE", "EDDLESTON", "EDENBRIDGE", "EDGEWORTH", "EDGWARE", "EDINBURGH",
"EDMONTON", "EGHAM", "ELGIN", "ELLESMERE", "ELLESMERE PORT", "ELLESTREE", "ELY", "ENFIELD",
"ENNISKILLEN", "EPPING", "EPSOM", "ERITH", "ESHER", "EVESHAM", "EXETER", "EXMOUTH",
"EYEMOUTH", "FAILSWORTH", "FAIRFORD", "FAKENHAM", "FALMOUTH", "FAREHAM", "FARNBOROUGH", "FARNHAM",
"FAVERSHAM", "FELIXSTOWE", "FELTHAM", "FERRYHILL", "FILEY", "FLEET", "FLEETWOOD", "FLINT",
"FOLKESTONE", "FORFAR", "FORMBY", "FORRES", "FORT WILLIAM", "FOWEY", "FRASERBURGH", "FRODSHAM",
"FROME", "GAINSBOROUGH", "GALASHIELS", "GARSTANG", "GATESHEAD", "GERRARDS CROSS", "GILLINGHAM", "GLASGOW", "GLASTONBURY",
"GLENROTHES", "GLOSSOP", "GLOUCESTER", "GODALMING", "GOLBORNE", "GOOLE", "GORLESTON", "GOSPORT", "GRANGEMOUTH",
"GRANTHAM", "GRANTOWN-ON-SPEY", "GRAVESEND", "GRAYS", "GREAT YARMOUTH", "GREENFORD", "GREENWICH", "GREENOCK", "GRIMSBY", "GUILDFORD",
"HAILSHAM", "HALESOWEN", "HALIFAX", "HALSTEAD", "HAMMERSMITH", "HARLECH", "HARLOW", "HARPENDEN",
"HARROGATE", "HARROW", "HARTLEPOOL", "HARWICH", "HASLEMERE", "HASTINGS", "HATFIELD", "HAVANT", "HAWES", 
"HAVERFORDWEST", "HAVERHILL", "HAVERING", "HAWARDEN", "HAWICK", "HAY-ON-WYE", "HAYWARDS HEATH", "HAZLEMERE",
"HEANOR", "HEATHFIELD", "HEBBURN", "HELENSBURGH", "HELSTON", "HEMEL HEMPSTEAD", "HENLEY-ON-THAMES", "HEREFORD",
"HERNE BAY", "HESSLE","HESWALL", "HEXHAM", "HEYWOOD", "HIGH WYCOMBE", "HINCKLEY", "HITCHIN", "HODDESDON",
"HOLMFIRTH", "HOLSWORTHY", "HOLT", "HONITON", "HORLEY", "HORNCHURCH", "HORNSEA", "HORSHAM",
"HOVE", "HOYLAKE", "HUDDERSFIELD", "HUNSTANTON", "HUNTINGDON", "HYDE", "HYTHE", "ILFORD", "IMMINGHAM",
"ILKESTON", "ILKLEY", "ILMINSTER", "INGLETON", "INNERLEITHEN", "INVERARAY", "INVERKEITHING", "INVERNESS", "INVERURIE",
"IPSWICH", "IRVINE", "IVER", "ISLEWORTH", "IVYBRIDGE", "JARROW", "JOHNSTONE", "KEIGHLEY", "KEMPSTON", "KENDAL",
"KENILWORTH", "KESGRAVE", "KESWICK", "KETTERING", "KEYNSHAM", "KIDDERMINSTER", "KIDLINGTON", "KILMARNOCK",
"KILSYTH", "KING'S LYNN", "KINGS LYNN", "KINGSBRIDGE", "KINGSTON UPON HULL", "KINGSTON UPON THAMES", "KINGSWINFORD", "KINGTON", "KINROSS",
"KIRKBY", "KIRKBY LONSDALE", "KIRKCALDY", "KIRKCUDBRIGHT", "KIRKHAM", "KIRKINTILLOCH", "KIRKWALL", "KNARESBOROUGH",
"KNOTTINGLEY", "KNUTSFORD", "LANARK", "LANCASTER", "LARGS", "LARKHALL", "LATCHFORD", "LAUNCESTON", "LAXEY",
"LEAMINGTON SPA", "LEATHERHEAD", "LEDBURY", "LEE-ON-THE-SOLENT", "LEEDS", "LEEK", "LEICESTER", "LEIGH","LEIGHTON BUZZARD",
"LEOMINSTER", "LESTON", "LETCHWORTH", "LEVEN", "LEWES", "LEWISHAM", "LEYLAND", "LICHFIELD",
"LIMAVADY", "LINCOLN", "LINFORD", "LINGFIELD", "LINLITHGOW", "LISBURN", "LISKEARD", "LITTLEHAMPTON",
"LIVERPOOL", "LIVINGSTON", "LLANDEILO", "LLANDOVERY", "LLANDRINDOD WELLS", "LLANELLI", "LLANGOLLEN", "LLANIDLOES",
"LOANHEAD", "LOCHGILPHEAD", "LOCKERBIE", "LONDON", "LONDONDERRY", "LONG EATON", "LONGRIDGE", "LOOE",
"LOSSIEMOUTH", "LOUGHTON", "LOUGHBOROUGH", "LOUTH", "LOWESTOFT", "LUDLOW", "LURGAN", "LUTON", "LYDNEY", "LYTHAM ST. ANNES", 
"LYME REGIS", "LYMINGTON", "LYTHAM ST ANNES", "MACCLESFIELD", "MAESTEG", "MAIDENHEAD", "MAIDSTONE", "MALDON",
"MALMESBURY", "MALTON", "MALVERN", "MANCHESTER", "MANNINGTREE", "MANSFIELD", "MARAZION", "MARCH",
"MARGATE", "MARKET DRAYTON", "MARKET HARBOROUGH", "MARKET RASEN", "MARLBOROUGH", "MARLOW", "MARTOCK", "MARYPORT",
"MATLOCK", "MAYBOLE", "MELKSHAM", "MELROSE", "MEIFOD", "MELTON MOWBRAY", "MERTHYR TYDFIL", "MEXBOROUGH", "MIDDLEHAM",
"MIDDLESBROUGH", "MIDDLEWICH", "MIDHURST", "MIDSOMER NORTON", "MILFORD HAVEN", "MILLPORT", "MILNGAVIE", "MILNTHORPE",
"MILTON KEYNES", "MINEHEAD", "MOFFAT", "MOLD", "MONMOUTH", "MONTROSE", "MORECAMBE", "MORETON-IN-MARSH",
"MORETONHAMPSTEAD", "MORLEY", "MORPETH", "MOTHERWELL", "MOUNTAIN ASH", "MUCH WENLOCK", "MUSSELBURGH", "NAILSWORTH",
"NAIRN", "NANTWICH", "NEATH", "NEEDHAM MARKET", "NESTON", "NEW MILTON", "NEWARK-ON-TRENT", "NEWBURY",
"NEWCASTLE", "NEWCASTLE EMLYN", "NEWCASTLE UNDER LYME", "NEWCASTLE UPON TYNE", "NEWENT", "NEWMARKET", "NEWPORT","NEWPORT PAGNELL", "NEWQUAY",
"NEWRY", "NEWTON ABBOT", "NEWTON AYCLIFFE", "NEWTON STEWART", "NEWTON-LE-WILLOWS", "NEWTOWN", "NEWTOWNABBEY", "NEWTOWNARDS", "NORBURY"
"NORMANTON", "NORTH WALSHAM", "NORTHALLERTON", "NORTHAM", "NORTHAMPTON", "NORTHFLEET", "NORTH SHIELDS","NORTHWICH", "NORWICH",
"NOTTINGHAM", "NUNEATON", "OADBY", "OAKHAM", "OBAN", "OCKLEY", "OKEHAMPTON", "OLDHAM", "OLDBURY",
"OLNEY", "OMAGH", "ONGAR", "ONCHAN", "ORFORD", "ORMSKIRK", "ORPINGTON", "OSSETT", "OSWESTRY",
"OTLEY", "OTTERY ST MARY", "OUNDLE", "OUTER HEBRIDES", "OXFORD", "PADSTOW", "PAIGNTON", "PAISLEY", "PAR, CORNWALL",
"PEEL, ISLE OF MAN", "PEEBLES", "PEEL", "PEMBROKE", "PENARTH", "PENICUIK", "PENISTONE", "PENRITH", "PENRYN", "PENZANCE",
"PERRANPORTH", "PERSHORE", "PERTH", "PETERBOROUGH", "PETERHEAD", "PETERLEE", "PETERSFIELD", "PETWORTH",
"PEVENSEY", "PICKERING", "PITLOCHRY", "PLYMOUTH", "POCKLINGTON", "POLEGATE", "PONTEFRACT", "PONTYPOOL", "PONTYPRIDD",
"POOLE", "PORT TALBOT", "PORTADOWN", "PORTAFERRY", "PORTHCAWL", "PORTHMADOG", "PORTISHEAD", "PORTLAND", "PORTREE",
"PORTSMOUTH", "PORTSTEWART", "POTTEN END", "POTTERS BAR", "POTTON", "POULTON-LE-FYLDE", "PRESCOT", "PRESTATYN", "PRESTWICK",
"PRESTEIGNE", "PRESTON", "PRINCES RISBOROUGH", "PRUDHOE", "PUDDLETOWN", "PUDSEY", "PURFLEET", "PURLEY",
"PUTNEY", "PWLLHELI", "RAMSBOTTOM", "RAMSEY", "RAMSGATE", "RAMSGILL", "RAUNDS", "RAWDON",
"RAYLEIGH", "READING", "REDCAR", "REDDITCH", "REDHILL", "REETH", "REIGATE", "RENFR_REPEAT", "RET REPEAT",
"RETFORD", "RETREAT", "RHYL", "RICHMOND", "RICKMANSWORTH", "RINGWOOD", "RIPLEY", "RIPON",
"ROBERTSBRIDGE", "ROCHESTER", "ROCHDALE", "ROMFORD", "ROMSEY", "ROSS-ON-WYE", "ROTHBURY", "ROTHERHAM", "ROTHESAY",
"ROWLEY REGIS", "ROYAL LEAMINGTON SPA", "ROYAL TUNBRIDGE WELLS", "ROYAL WOOTTON BASSETT", "ROYSTON", "ROYTON", "RUGBY", "RUGELEY", "RUNCORN",
"RUSHDEN", "RUTHIN", "RYDE", "RYE", "SADDLEWORTH", "SAFFRON WALDEN", "SALCOMBE", "SALE",
"SALFORD", "SALISBURY", "SALTASH", "SALTBURN-BY-THE-SEA", "SANDOWN", "SANDWICH", "SANDY", "SAWBRIDGEWORTH",
"SAXMUNDHAM", "SCARBOROUGH", "SCUNTHORPE", "SEAFORD", "SEAHAM", "SEASCALE", "SEATON", "SEDGEFIELD",
"SELBY", "SELKIRK", "SENNEN", "SETTLE", "SEVENOAKS", "SHAFTESBURY", "SHANKLIN", "SHEFFIELD",
"SHEFFORD", "SHEPTON MALLET", "SHERBORNE", "SHERINGHAM", "SHILDON", "SHIFNAL", "SHIPSTON-ON-STOUR", "SHOREHAM-BY-SEA", "SHREWSBURY",
"SIDMOUTH", "SITTINGBOURNE", "SKEGNESS", "SKELMERSDALE", "SKIPTON", "SLEAFORD", "SLOUGH", "SMETHWICK",
"SNAITH", "SOHAM", "SOLIHULL", "SOMERTON", "SOUTH MOLTON", "SOUTH SHIELDS", "SOUTHAM", "SOUTHALL", "SOUTHAMPTON", "SOUTHSEA",
"SOUTHEND-ON-SEA", "SOUTHPORT", "SOUTHWELL", "SOUTHWICK", "SOWERBY BRIDGE", "SPALDING", "SPENNYMOOR", "SPILSBY",
"ST ALBANS", "ST AUSTELL", "ST COLUMB MAJOR", "ST HELENS", "STHELENS", "ST IVES", "ST NEOTS", "STAFFORD", "STAINES",
"STAINES-UPON-THAMES", "STAINESUPONTHAMES", "STAITHES", "STALBRIDGE", "STALHAM", "STALYBRIDGE", "STAMFORD", "STANLEY", "STANFORDLEHOPE", "STAPLEFORD",
"STAVELEY", "STEVENAGE", "STEYNING", "STOCKPORT", "STOCKTON-ON-TEES", "STOKE-ON-TRENT", "STONE", "STONEHAVEN", "STOKEONTRENT",
"STOURBRIDGE", "STOURPORT-ON-SEVERN", "STOW-ON-THE-WOLD", "STOWMARKET", "STRABANE", "STRANRAER", "STRATFORD-UPON-AVON", "STRATFORDUPONAVON", "STREET",
"STROUD", "STURMINSTER NEWTON", "SUDBURY", "SUNDERLAND", "SUNBURYONTHAMES", "SURBITON", "SUTTON", "SUTTON COLDFIELD", "SUTTON-IN-ASHFIELD",
"SWADLINCOTE", "SWAFFHAM", "SWANAGE", "SWANLEY", "SWANSEA", "SWINDON", "TADCASTER", "TADLEY",
"TAIN", "TALGARTH", "TAMWORTH", "TAUNTON", "TAVISTOCK", "TEIGNMOUTH", "TELFORD", "TENBY",
"TENTERDEN", "TETBURY", "TEWKESBURY", "THAME", "THANET", "THATCHAM", "THAXTED", "THETFORD",
"THIRSK", "THORNBURY", "THORNE", "THORNTON", "THORNTON HEATH", "THORNTON-CLEVELEYS", "THORPENESS", "THRAPSTON",
"TIDWORTH", "TILBURY", "TILICOUTRY", "TIPPERARY", "TIPTON", "TIREE", "TIVERTON", "TOBERMORY",
"TODMORDEN", "TONBRIDGE", "TOPSHAM", "TORPOINT", "TORQUAY", "TOTNES", "TOTTENHAM", "TOTTON",
"TOWCESTER", "TOWYN", "TRAFFORD", "TREDEGAR", "TREGARON", "TREHARRIS", "TRING", "TROON",
"TROWBRIDGE", "TRURO", "TUNBRIDGE WELLS", "TWICKENHAM", "TYLDESLEY", "TYNEMOUTH", "TYWYN", "UCKFIELD",
"ULVERSTON", "UMBERLEIGH", "UTTOXETER", "UXBRIDGE", "VENTNOR", "VERWOOD", "WADHURST", "WAKEFIELD",
"WALLASEY", "WALLINGFORD", "WALLSEND", "WALSALL", "WALTHAM ABBEY", "WALTHAM CROSS", "WALTHAMSTOW", "WALTON-ON-THAMES",
"WALTON-ON-THE-NAZE", "WANDSWORTH", "WANTAGE", "WARE", "WAREHAM", "WARMINSTER", "WARRINGTON", "WARWICK",
"WASHINGTON", "WATCHET", "WATERLOOVILLE", "WATFORD", "WEDNESBURY", "WELLINGBOROUGH", "WELLS", "WELLS-NEXT-THE-SEA",
"WELSHPOOL", "WELWYN GARDEN CITY", "WEMBLEY", "WENDOVER", "WEST BRIDGFORD", "WESTONSUPERMARE", "WEST BROMWICH", "WESTBURY", "WESTGATE-ON-SEA","WESTERHAM",
"WESTON-SUPER-MARE", "WEYMOUTH", "WHITBY", "WHITCHURCH", "WHITEHAVEN", "WHITLEY BAY", "WHITNASH", "WHITSTABLE",
"WHITTLESEY", "WHITWORTH", "WICK", "WICKFORD", "WIDNES", "WIGAN", "WIGSTON", "WIGTON",
"WILLENHALL", "WILLINGTON", "WILMSLOW", "WILTON", "WIMBORNE MINSTER", "WINCANTON", "WINCHELSEA", "WINCHESTER",
"WINDERMERE", "WINDSOR", "WINSFORD", "WINSLOW", "WIRKSWORTH", "WISBECH", "WITHAM", "WITHERNSEA",
"WITNEY", "WOBURN", "WOKING", "WOKINGHAM", "WOLVERHAMPTON", "WOMBWELL", "WOODBRIDGE", "WOODSTOCK", "WOTTONUNDEREDGE",
"WOOLER", "WORCESTER", "WORKINGTON", "WORKSOP", "WORLDS END", "WORTHING", "WOTTON-UNDER-EDGE", "WREXHAM",
"WYMONDHAM", "YARM", "YARMOUTH", "YATE", "YATELEY", "YEOVIL", "YORK"]

# This town list has 300 more distinct towns. the total number is now 967

contextual_keywords = [
    'AIRPORT', 'BEACH', 'BORO', 'BOROUGH', 'BRIDGE', 'CENTRAL', 'CHES', 'CITY', 
    'CNTY', 'COAST', 'COUNTY', 'DERBYS', 'DIST', 'DISTRICT', 'EAST', 'FOREST', 
    'GREATER', 'GTR', 'HAM', 'HANTS', 'HARBOUR', 'HILL', 'HILLS', "ICKENHAM", 'ISLAND', 'LAKE', 
    'LANCS', 'LANE', 'LINCS', 'LOWER', 'MOUNT', 'N.I.', 'NEW', 'NI', 'NORTH', 'NOTTS', 
    'NR', 'OLD', 'PARISH', 'PARK', 'PEAK', 'PORT', 'REGION', 'RIVER', 'ROAD', 'SC', 
    'SCOT', 'SOUTH', 'SQUARE', 'STATION', 'SUFF', 'SUSS', 'TON', 'TOWN', 
    'UPPER', 'VALLEY', 'VILLAGE', 'WALES', 'WARD', 'WELSH', 'WEST', 'YK', 'YORKS'
]

# adding this list in for just the parser part. it will likely take a long time to compile 
village_list = [
    "ABERFORD", "BAMBURGH", "CHAGFORD", "DENT", "EARDISLAND", "FOWEY", "GRASMERE",
    "HAWORTH", "ILAM", "JERRETSPASS", "KINETON", "LUSTLEIGH", "MORTONHAMPSTEAD",
    "NETHER STOWEY", "OAKHAM", "PORLOCK", "QUINTRELL DOWNS", "ROTHBURY", "STANTON DREW",
    "TINTAGEL", "UPLYME", "VENTNOR", "WARKWORTH", "YALDING", "ZENNOR", "ALNMOUTH",
    "BEER", "CERNE ABBAS", "DUNSTER", "EYAM", "FRAMPTON ON SEVERN", "GODSHILL",
    "HESWALL", "INKBERROW", "JUNIPER GREEN", "KIRKBY LONSDALE", "LACOCK", "MIDDLEHAM",
    "NORTH PETHERWIN", "OTTERBURN", "PLOCKTON", "QUORN", "RIPPLE", "SOUTH PETHERTON",
    "THAXTED", "UFFINGTON", "VOWCHURCH", "EXFORD", "YETMINSTER", "ZEALS", "AMBLESIDE",
    "BROUGHTON IN FURNESS", "CASTLE COMBE", "DUNWICH", "ELSTOW", "FULBROOK", "GIGGLESWICK",
    "HORNING", "INGLETON", "JACOBSTOWE", "KELMSCOTT", "LONG MELFORD", "MINCHINHAMPTON",
    "NEWTONMORE", "ORFORD", "POLPERRO", "QUAINTON", "REETH", "ST MARY BOURNE", "TINTERN",
    "UPPER SLAUGHTER", "VALE OF HEALTH", "WADDINGTON", "YARNSCOMBE", "ZOUCH", "ASHWELL",
    "BLAKENEY", "CRANFORD", "DITTISHAM", "EASTNOR", "FELBRIGG", "GOUDHURST", "HUNSTANTON",
    "ICKFORD", "JEDBURGH", "KNOCKHOLT", "LAVENHAM", "MUCH WENLOCK", "NETHERBURY", "OARE",
    "PRESTEIGNE", "QUEDGELEY", "RUDGLEY", "SHIPSTON-ON-STOUR", "TURVILLE", "UFFORD",
    "VOLUNTEER GREEN", "WHITCHURCH", "YAXLEY", "ALDBROUGH", "BIDDESTONE", "CHILHAM",
    "DRAYTON", "EGLWYSWRW", "FINDHORN", "GLENARM", "HARTINGTON", "IRVINETOWN", "KIRKCUDBRIGHT",
    "LOCHINVER", "MELLS", "NARBERTH", "OXFORD", "PEEBLES", "QUOTHQUAN", "ROSCARROCK",
    "STIFFKEY", "TYNDRUM", "UIG", "VATISKER", "WOOKEY HOLE", "YSTRADGYNLAIS", "ZELAH"
]
 
  
# expand on town_list
alternative_town_list = [
"JERSEY, CHANNEL ISLANDS", "SHIPSTON ON STOUR", "SHIPSTON_ON_STOUR", "BISHOPS STORTFORD", 
"PRESTON, LANCS", "PRESTON, LANCASHIRE", "RAMSEY, ISLE-OF-MAN", "RAMSEY, ISLE OF MAN", 
"STANFORD LE HOPE", "STANFORD_LE_HOPE", "SUTTON_IN_ASHFIELD", "SUTTON IN ASHFIELD", 
"KIRKBY IN ASHFIELD", "KIRKBY_IN_ASHFIELD", "STOKE ON TRENT", "STOKE_ON_TRENT",
"ASHBY DE LA ZOUCH", "ASHBY_DE_LA_ZOUCH", "STOKE ON-TRENT", "BURNHAM ON SEA", "BURNHAM_ON_SEA",
"SUTTON IN ASHFIELD", "SUTTON_IN_ASHFIELD", "BURTON ON TRENT", "BURTON_ON_TRENT",
"STAINES UPON THAMES", "STAINES_UPON_THAMES", "NEWCASTLE UPON TYNE", "NEWCASTLE_UPON_TYNE", "PAR, CORNWALL"
"WIRRAL, MERSEYSIDE"  "WESTGATE ON SEA", "WESTGATE_ON_SEA", "LYTHAM ST ANNES", "LYTHAM_ST_ANNES", "KINGS LYNN"
"WELLS NEXT THE SEA", "WELLS_NEXT_THE_SEA", "WALTON ON THAMES", "WALTON_ON_THAMES", "PEEL, ISLE OF MAN", "CHELMSFORD ESSEX"
]

allowed_country_list = [
        "ENGLAND", "SCOTLAND", "WALES", "NORTHERN IRELAND", "IRELAND", "N IRELAND", 
    ]
  # variations of Northern Ireland
county_list = [
    "ABERDEEN CITY", "ABERDEENSHIRE", "ANGUS", "ANTRIM AND NEWTOWNABBEY", "ARGYLL AND BUTE", "ARMAGH CITY, BANBRIDGE AND CRAIGAVON",
    "BEDFORDSHIRE", "BELFAST", "BERKSHIRE", "BRISTOL", "BUCKINGHAMSHIRE", "CAMBRIDGESHIRE",
    "CAUSEWAY COAST AND GLENS", "CHESHIRE", "CITY OF LONDON", "CLACKMANNANSHIRE", "CORNWALL", "CUMBRIA",
    "DERBYSHIRE", "DERRY CITY AND STRABANE", "DEVON", "DORSET", "DUMFRIES AND GALLOWAY", "DUNDEE CITY",
    "DURHAM", "EAST AYRSHIRE", "EAST DUNBARTONSHIRE", "EAST LOTHIAN", "EAST RENFREWSHIRE", "EAST RIDING OF YORKSHIRE",
    "EAST SUSSEX", "EDINBURGH", "ESSEX", "FALKIRK", "FERMANAGH AND OMAGH", "FIFE", 
    "GLASGOW CITY", "GLOUCESTERSHIRE", "GREATER LONDON", "GREATER MANCHESTER", "GWYNEDD","HAMPSHIRE",
    "HERTFORDSHIRE", "HIGHLAND", "INVERCLYDE", "ISLE OF WIGHT", "KENT", "LANCASHIRE",
    "LEICESTERSHIRE", "LINCOLNSHIRE", "LISBURN AND CASTLEREAGH", "MERSEYSIDE", "MID AND EAST ANTRIM", "MID ULSTER",
    "MIDLOTHIAN", "MORAY", "NA H-EILEANAN SIAR", "NEWRY, MOURNE AND DOWN", "NORFOLK", "NORTH AYRSHIRE",
    "NORTH LANARKSHIRE", "NORTH YORKSHIRE", "NORTHAMPTONSHIRE", "NORTHUMBERLAND", "NOTTINGHAMSHIRE", "ORKNEY ISLANDS",
    "OXFORDSHIRE", "PERTH AND KINROSS", "RENFREWSHIRE", "RUTLAND", "SCOTTISH BORDERS", "SHETLAND ISLANDS",
    "SHROPSHIRE", "SOMERSET", "SOUTH AYRSHIRE", "SOUTH LANARKSHIRE", "SOUTH YORKSHIRE", "STAFFORDSHIRE",
    "STIRLING", "SUFFOLK", "SURREY", "TYNE AND WEAR", "WARWICKSHIRE", "WEST DUNBARTONSHIRE",
    "WEST LOTHIAN", "WEST MIDLANDS", "WEST SUSSEX", "WEST YORKSHIRE", "WILTSHIRE", "WORCESTERSHIRE"
]
# expand on county_list/ principal areas, 11 counties, 11 county boroughs, 8 ceremonial counties
# expand to scotland/northern Island , grab those... expand counties to be more inclusive
disallowed_country_list = [
    "AFGHANISTAN", "ALBANIA", "ALGERIA", "AMERICA", "ANDORRA", "ANGOLA", "ANTIGUA AND BARBUDA", "ARGENTINA",
    "ARMENIA", "AUSTRALIA", "AUSTRIA", "AZERBAIJAN", "BAHAMAS", "BAHRAIN", "BANGLADESH", "BARBADOS",
    "BELARUS", "BELGIUM", "BELIZE", "BENIN", "BHUTAN", "BOLIVIA", "BOSNIA AND HERZEGOVINA", "BOTSWANA",
    "BRAZIL", "BRUNEI", "BULGARIA", "BURKINA FASO", "BURUNDI", "CABO VERDE", "CAMBODIA", "CAMEROON",
    "CANADA", "CENTRAL AFRICAN REPUBLIC", "CHAD", "CHILE", "CHINA", "COLOMBIA", "COMOROS", "CONGO",
    "COSTA RICA", "COTE D'IVOIRE", "CROATIA", "CUBA", "CYPRUS", "CZECHIA", "DENMARK", "DJIBOUTI",
    "DOMINICA", "DOMINICAN REPUBLIC", "ECUADOR", "EGYPT", "EL SALVADOR", "EQUATORIAL GUINEA", "ERITREA",
    "ESTONIA", "ESWATINI", "ETHIOPIA", "FIJI", "FINLAND", "FRANCE", "GABON", "GAMBIA", "GEORGIA",
    "GERMANY", "GHANA", "GREECE","GREENLAND", "GRENADA", "GUATEMALA", "GUINEA", "GUINEA-BISSAU", "GUYANA", "HAITI",
    "HONDURAS", "HONG KONG" , "HUNGARY", "ICELAND", "INDIA", "INDONESIA", "IRAN", "IRAQ", "ISRAEL",
    "ITALY", "JAMAICA", "JAPAN", "JORDAN", "KAZAKHSTAN", "KENYA", "KIRIBATI", "KOREA", "KOSOVO", "KUWAIT",
    "KYRGYZSTAN", "LAOS", "LATVIA", "LEBANON", "LESOTHO", "LIBERIA", "LIBYA", "LIECHTENSTEIN",
    "LITHUANIA", "LUXEMBOURG", "MACAU", "MADAGASCAR", "MALAWI", "MALAYSIA", "MALDIVES", "MALI", "MALTA",
    "MARSHALL ISLANDS", "MAURITANIA", "MAURITIUS", "MEXICO", "MICRONESIA", "MOLDOVA", "MONACO",
    "MONGOLIA", "MONTENEGRO", "MOROCCO", "MOZAMBIQUE", "MYANMAR", "NAMIBIA", "NAURU", "NEPAL",
    "NETHERLANDS", "NEW ZEALAND", "NICARAGUA", "NIGER", "NIGERIA", "NORTH KOREA", "NORTH MACEDONIA",
    "NORWAY", "OMAN", "PAKISTAN", "PALAU", "PALESTINE", "PANAMA", "PAPUA NEW GUINEA", "PARAGUAY",
    "PERU", "PHILIPPINES", "POLAND", "PORTUGAL", "QATAR", "ROMANIA", "RUSSIA", "RUSSIAN FEDERATION", "RWANDA", "SAINT KITTS AND NEVIS",
    "SAINT LUCIA", "SAINT VINCENT AND THE GRENADINES", "SAMOA", "SAN MARINO", "SAO TOME AND PRINCIPE",
    "SAUDI ARABIA", "SENEGAL", "SERBIA", "SEYCHELLES", "SIERRA LEONE", "SINGAPORE", "SLOVAKIA",
    "SLOVENIA", "SOLOMON ISLANDS", "SOMALIA", "SOUTH AFRICA", "SOUTH KOREA", "SOUTH SUDAN", "SPAIN",
    "SRI LANKA", "SUDAN", "SURINAME", "SWEDEN", "SWITZERLAND", "SYRIA", "TAIWAN", "TAJIKISTAN",
    "TANZANIA", "THAILAND", "TIMOR-LESTE", "TOGO", "TONGA", "TRINIDAD AND TOBAGO", "TUNISIA",
    "TURKEY",  "TURKS AND CAICOS ISLANDS",  "TURKMENISTAN", "TUVALU", "UGANDA", "UKRAINE", "UNITED ARAB EMIRATES",
    "UNITED STATES", "URUGUAY", "UZBEKISTAN", "VANUATU", "VATICAN CITY", "VENEZUELA", "VIETNAM",
    "YEMEN", "ZAMBIA", "ZIMBABWE"]