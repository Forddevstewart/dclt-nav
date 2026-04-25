# Protected and Recreational OpenSpace (OPENSPACE_POLY)
General

Name
OpenSpace — OPENSPACE_POLY
Path
/Users/fordstewart/Downloads/openspace_gdb/OpenSpace.gdb
Last modified
February 7, 2026 6:05:50 PM EST
Source
/Users/fordstewart/Downloads/openspace_gdb/OpenSpace.gdb|layername=OPENSPACE_POLY
Provider
ogr
Layer ID
OPENSPACE_POLY_f1639fc8_7a0b_4d42_9aa2_5bb7a73972d3



Information from provider

Storage
OpenFileGDB
Comment
OpenSpace (Areas)
Encoding
UTF-8
Geometry
Polygon (MultiPolygon)
Extent
33863.7325000017881393,777671.9644999988377094 : 330782.2942000031471252,959328.1572000011801720
Feature count
60,150



Coordinate Reference System (CRS)

Name
EPSG:26986 - NAD83 / Massachusetts Mainland
Units
meters
Type
Projected
Method
Lambert Conformal Conic
Celestial Body
Earth
Reference
Static (relies on a datum which is plate-fixed)



Identification

Identifier
GISDATA.OPENSPACE_POLY
Parent Identifier

Title
Protected and Recreational OpenSpace (Polygons)
Type
dataset
Language
ENG
Abstract
The protected and recreational open space datalayer contains the boundaries of conservation lands and outdoor recreational facilities in Massachusetts. The associated database contains relevant information about each parcel, including ownership, level of protection, public accessibility, assessor’s map and lot numbers, and related legal interests held on the land, including conservation restrictions. Conservation and outdoor recreational facilities owned by federal, state, county, municipal, and nonprofit enterprises are included in this datalayer. Not all lands in this layer are protected in perpetuity, though nearly all have at least some level of protection.
Although the initial data collection effort for this data layer has been completed, open space changes continually and this data layer is therefore considered to be under development. Additionally, due to the collaborative nature of this data collection effort, the accuracy and completeness of open space data varies across the state’s municipalities. Attributes, while comprehensive in scope, may be incomplete for many parcels.
The OpenSpace layer includes two feature classes:
OPENSPACE_POLY - polygons of recreational and conservation lands as described above
OPENSPACE_ARC - attributed lines that represent boundaries of the polygons
These feature classes are stored in an ArcSDE feature dataset named OPENSPACE that includes ArcGIS geodatabase topology. 
OPENSPACE_POLY - The following types of land are included in this datalayer:
conservation land- habitat protection with minimal recreation, such as walking trails 
recreation land- outdoor facilities such as town parks, commons, playing fields, school fields, golf courses, bike paths, scout camps, and fish and game clubs. These may be privately or publicly owned facilities. 
town forests 
parkways - green buffers along roads, if they are a recognized conservation resource 
agricultural land- land protected under an Agricultural Preservation Restriction (APR) and administered by the state Department of Agricultural Resources (DAR, formerly the Dept. of Food and Agriculture (DFA)) 
aquifer protection land - not zoning overlay districts 
watershed protection land - not zoning overlay districts 
cemeteries - if a recognized conservation or recreation resource
forest land -- if designated as a Forest Legacy Area

Map display, planning and analysis
Categories

Keywords

Vocabulary
Items
Search keys
open space, conservation, recreation, parks




Extent

CRS
EPSG:26986 - NAD83 / Massachusetts Mainland - Projected
Spatial Extent
CRS: EPSG:26986 - NAD83 / Massachusetts Mainland - Projected
X Minimum: -73.53319700000000125
Y Minimum: 41.2317600000000013
X Maximum: -69.89935400000000243
Y Maximum: 42.88457700000000017

Temporal Extent
Start: 2026-02-07T05:00:00Z
End: 
Start: 
End: 



Access

Fees

Licenses

Rights
MassGIS, Executive Office of Energy and Environmental Affairs
Constraints
Limitations of use: These data are very useful for most statewide and regional planning purposes. However, they are not a legal record of ownership, and the user should understand that parcel representations are generally not based on property surveys.



Fields

Primary key attributes
OBJECTID 
Count
62


Field
Type
Length
Precision
Comment
OBJECTID
Integer64
0
0

TOWN_ID
Int16
0
0

POLY_ID
Integer
0
0

SITE_NAME
String
120
0

FEE_OWNER
String
100
0

OWNER_ABRV (OWNER_ABBRV)
String
20
0

OWNER_TYPE
String
1
0

MANAGER
String
100
0

MANAGR_ABRV (MANAGER_ABBRV)
String
20
0

MANAGR_TYPE (MANAGER_TYPE)
String
1
0

PRIM_PURP (PRIMARY_PURPOSE)
String
1
0

PUB_ACCESS (PUBLIC_ACCESS)
String
1
0

LEV_PROT (LEVEL_PROTECTION)
String
1
0

OLI_1_ORG
String
100
0

OLI_1_ABRV (OLI_1_ABBRV)
String
20
0

OLI_1_TYPE
String
1
0

OLI_1_INT
String
20
0

OLI_2_ORG
String
100
0

OLI_2_ABRV (OLI_2_ABBRV)
String
20
0

OLI_2_TYPE
String
1
0

OLI_2_INT
String
20
0

OLI_3_ORG
String
100
0

OLI_3_ABRV (OLI_3_ABBRV)
String
20
0

OLI_3_TYPE
String
1
0

OLI_3_INT
String
20
0

GRANTPROG1
String
20
0

GRANTTYPE1
String
1
0

GRANTPROG2
String
20
0

GRANTTYPE2
String
1
0

PROJ_ID1
String
20
0

PROJ_ID2
String
20
0

PROJ_ID3
String
20
0

EOEAINVOLVE
Int16
0
0

ARTICLE97
Int16
0
0

FY_FUNDING
Int16
0
0

GIS_ACRES
Real
0
0

DEED_ACRES
Real
0
0

OS_DEED_BOOK
Integer
0
0

OS_DEED_PAGE
Integer
0
0

ASSESS_ACRE (ASSESSOR_ACRES)
Real
0
0

ASSESS_MAP (ASSESSOR_MAP)
String
10
0

ASSESS_BLK (ASSESSOR_BLOCK)
String
120
0

ASSESS_LOT (ASSESSOR_LOT)
String
10
0

ASSESS_SUB (ASSESSOR_SUBLOT)
String
10
0

ALT_SITE_NAME (ALTERNATE_SITE_NAME)
String
120
0

ATT_DATE
DateTime
0
0

BASE_MAP
String
10
0

SOURCE_MAP
String
50
0

SOURCE_TYPE
String
10
0

COMMENTS
String
255
0

LOC_ID
String
15
0

DCAM_ID
Integer
0
0

FEESYM (FEE_OWNER_SYMBOL)
String
20
0

INTSYM (OLI_SYMBOL)
String
20
0

OS_ID
String
9
0

CAL_DATE_R (CALENDAR_DATE_RECORDED)
DateTime
0
0

FORMAL_SITE_NAME
String
120
0

CR_REF
Integer
0
0

OS_TYPE
String
50
0

EEA_CR_ID
String
50
0

SHAPE_Length
Real
0
0

SHAPE_Area
Real
0
0




Contacts

ID
Name
Position
Organization
Role
Email
Voice
Fax
Addresses
1
Ben Smith
GIS Analyst / DBA, Protected & Recreational Open Space Program
Executive Office of Energy and Environmental Affairs

benjamin.smith@state.ma.us
(617) 626-1076

both
100 Cambridge St., 10th Floor
02114
Boston
MA
USA



Links

No links yet.



History

ID
Action
1
Editors of protected open spaces that include the Division of Fish and Game, the Department of Agriculture, the Department of Conservation and Recreation and the Division of Conservation Services, have continued to improve the dataset by adding many new fee-acquisitions or restrictions that were acquired in FY 2013. Acquisitions for FY 2014 have also begun to be digitized. Dozens of new conservation restrictions that were acquired in-part by the new Conservation Land Tax Credit program have been digitized as well.



