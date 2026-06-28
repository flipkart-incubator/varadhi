# Topic Failover — Base Case Timeline

**Date:** 2026-07-20 16:50:11 
**Topic:** `default_project.failover_grouped` (grouped, `X_GROUP_ID=g1`)
**Direction:** region-a → region-b
**Raw log:** `failover_validation_report_20260720_164932.log`

## Outcome

| Check | Result |
|-------|--------|
| Functional pass | **Fail** |
| ZK producing region | `region-b` |
| ZK topicState | `Fenced` |
| Produce HTTP 200 | 290 |
| Produce HTTP 422 (Fenced) | 0 |
| Fenced window | — |
| First 200 after fence | — |

## Timeline (main scenario)

| Time | Event | API | HTTP | Response / payload | Notes |
|------|-------|-----|------|-------------------|-------|
| 16:49:32.757 | Health check | GET /v1/health-check | **200** | `"iam_ok"` |  |
| 16:49:33.586 | POST failover | POST /failover | **200** | `{"operationId":"ad03d757-913c-4587-b28e-57c2dbb5d1b5","version":0,"topicFqn":"default_project.failover_grouped","requestedBy":"thanos","s...` | region-a → region-b, waitLag=false |
| 16:49:33.641 | GET failover → PENDING | GET /failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":0,"transitionKind":"FAILOVER","operationId":"ad03d757-913c-4587-b28e-57c2dbb5d1b5","sourceRegion":"region-a","targetRegi...` | topicVersionToAwait=0 |
| 16:49:33.687 | produce #3 | POST /produce | **200** | `"load-failover_grouped-2-1784546373603"` |  |
| 16:49:33.820 | GET failover → SWITCH | GET /failover | **200** | `{"topicFqn":"default_project.failover_grouped","version":2,"transitionKind":"FAILOVER","operationId":"ad03d757-913c-4587-b28e-57c2dbb5d1b5","sourceRegion":"region-a","targetRegi...` | topicVersionToAwait=33 |
| 16:49:33.930 | produce #4 | POST /produce | **200** | `"load-failover_grouped-3-1784546373869"` |  |
| 16:49:34.092 | produce #5 | POST /produce | **200** | `"load-failover_grouped-4-1784546374043"` |  |
| 16:49:34.253 | produce #6 | POST /produce | **200** | `"load-failover_grouped-5-1784546374201"` |  |
| 16:49:34.400 | produce #7 | POST /produce | **200** | `"load-failover_grouped-6-1784546374360"` |  |
| 16:49:34.532 | produce #8 | POST /produce | **200** | `"load-failover_grouped-7-1784546374489"` |  |
| 16:49:34.656 | produce #9 | POST /produce | **200** | `"load-failover_grouped-8-1784546374618"` |  |
| 16:49:34.789 | produce #10 | POST /produce | **200** | `"load-failover_grouped-9-1784546374745"` |  |
| 16:49:34.958 | produce #11 | POST /produce | **200** | `"load-failover_grouped-10-1784546374913"` |  |
| 16:49:35.551 | produce #12 | POST /produce | **200** | `"load-failover_grouped-11-1784546375410"` |  |
| 16:49:35.777 | produce #13 | POST /produce | **200** | `"load-failover_grouped-12-1784546375720"` |  |
| 16:49:36.336 | produce #14 | POST /produce | **200** | `"load-failover_grouped-13-1784546376263"` |  |
| 16:49:36.516 | produce #15 | POST /produce | **200** | `"load-failover_grouped-14-1784546376458"` |  |
| 16:49:36.659 | produce #16 | POST /produce | **200** | `"load-failover_grouped-15-1784546376615"` |  |
| 16:49:36.819 | produce #17 | POST /produce | **200** | `"load-failover_grouped-16-1784546376780"` |  |
| 16:49:36.937 | produce #18 | POST /produce | **200** | `"load-failover_grouped-17-1784546376904"` |  |
| 16:49:37.056 | produce #19 | POST /produce | **200** | `"load-failover_grouped-18-1784546377023"` |  |
| 16:49:37.181 | produce #20 | POST /produce | **200** | `"load-failover_grouped-19-1784546377140"` |  |
| 16:49:37.297 | produce #21 | POST /produce | **200** | `"load-failover_grouped-20-1784546377265"` |  |
| 16:49:37.413 | produce #22 | POST /produce | **200** | `"load-failover_grouped-21-1784546377379"` |  |
| 16:49:37.530 | produce #23 | POST /produce | **200** | `"load-failover_grouped-22-1784546377495"` |  |
| 16:49:37.646 | produce #24 | POST /produce | **200** | `"load-failover_grouped-23-1784546377612"` |  |
| 16:49:37.780 | produce #25 | POST /produce | **200** | `"load-failover_grouped-24-1784546377729"` |  |
| 16:49:37.918 | produce #26 | POST /produce | **200** | `"load-failover_grouped-25-1784546377879"` |  |
| 16:49:38.059 | produce #27 | POST /produce | **200** | `"load-failover_grouped-26-1784546378026"` |  |
| 16:49:38.193 | produce #28 | POST /produce | **200** | `"load-failover_grouped-27-1784546378145"` |  |
| 16:49:38.318 | produce #29 | POST /produce | **200** | `"load-failover_grouped-28-1784546378275"` |  |
| 16:49:38.487 | produce #30 | POST /produce | **200** | `"load-failover_grouped-29-1784546378433"` |  |
| 16:49:38.616 | produce #31 | POST /produce | **200** | `"load-failover_grouped-30-1784546378574"` |  |
| 16:49:38.737 | produce #32 | POST /produce | **200** | `"load-failover_grouped-31-1784546378702"` |  |
| 16:49:38.869 | produce #33 | POST /produce | **200** | `"load-failover_grouped-32-1784546378834"` |  |
| 16:49:38.992 | produce #34 | POST /produce | **200** | `"load-failover_grouped-33-1784546378956"` |  |
| 16:49:39.111 | produce #35 | POST /produce | **200** | `"load-failover_grouped-34-1784546379077"` |  |
| 16:49:39.238 | produce #36 | POST /produce | **200** | `"load-failover_grouped-35-1784546379198"` |  |
| 16:49:39.359 | produce #37 | POST /produce | **200** | `"load-failover_grouped-36-1784546379324"` |  |
| 16:49:39.505 | produce #38 | POST /produce | **200** | `"load-failover_grouped-37-1784546379470"` |  |
| 16:49:39.624 | produce #39 | POST /produce | **200** | `"load-failover_grouped-38-1784546379587"` |  |
| 16:49:39.784 | produce #40 | POST /produce | **200** | `"load-failover_grouped-39-1784546379748"` |  |
| 16:49:39.902 | produce #41 | POST /produce | **200** | `"load-failover_grouped-40-1784546379865"` |  |
| 16:49:40.023 | produce #42 | POST /produce | **200** | `"load-failover_grouped-41-1784546379988"` |  |
| 16:49:40.333 | produce #43 | POST /produce | **200** | `"load-failover_grouped-42-1784546380291"` |  |
| 16:49:40.461 | produce #44 | POST /produce | **200** | `"load-failover_grouped-43-1784546380415"` |  |
| 16:49:40.600 | produce #45 | POST /produce | **200** | `"load-failover_grouped-44-1784546380565"` |  |
| 16:49:40.740 | produce #46 | POST /produce | **200** | `"load-failover_grouped-45-1784546380696"` |  |
| 16:49:40.861 | produce #47 | POST /produce | **200** | `"load-failover_grouped-46-1784546380821"` |  |
| 16:49:40.979 | produce #48 | POST /produce | **200** | `"load-failover_grouped-47-1784546380942"` |  |
| 16:49:41.094 | produce #49 | POST /produce | **200** | `"load-failover_grouped-48-1784546381061"` |  |
| 16:49:41.209 | produce #50 | POST /produce | **200** | `"load-failover_grouped-49-1784546381179"` |  |
| 16:49:41.332 | produce #51 | POST /produce | **200** | `"load-failover_grouped-50-1784546381301"` |  |
| 16:49:41.473 | produce #52 | POST /produce | **200** | `"load-failover_grouped-51-1784546381417"` |  |
| 16:49:41.626 | produce #53 | POST /produce | **200** | `"load-failover_grouped-52-1784546381591"` |  |
| 16:49:41.745 | produce #54 | POST /produce | **200** | `"load-failover_grouped-53-1784546381711"` |  |
| 16:49:41.859 | produce #55 | POST /produce | **200** | `"load-failover_grouped-54-1784546381827"` |  |
| 16:49:41.972 | produce #56 | POST /produce | **200** | `"load-failover_grouped-55-1784546381941"` |  |
| 16:49:42.094 | produce #57 | POST /produce | **200** | `"load-failover_grouped-56-1784546382062"` |  |
| 16:49:42.207 | produce #58 | POST /produce | **200** | `"load-failover_grouped-57-1784546382178"` |  |
| 16:49:42.326 | produce #59 | POST /produce | **200** | `"load-failover_grouped-58-1784546382295"` |  |
| 16:49:42.442 | produce #60 | POST /produce | **200** | `"load-failover_grouped-59-1784546382409"` |  |
| 16:49:42.555 | produce #61 | POST /produce | **200** | `"load-failover_grouped-60-1784546382524"` |  |
| 16:49:42.673 | produce #62 | POST /produce | **200** | `"load-failover_grouped-61-1784546382636"` |  |
| 16:49:42.791 | produce #63 | POST /produce | **200** | `"load-failover_grouped-62-1784546382759"` |  |
| 16:49:42.962 | produce #64 | POST /produce | **200** | `"load-failover_grouped-63-1784546382882"` |  |
| 16:49:43.090 | produce #65 | POST /produce | **200** | `"load-failover_grouped-64-1784546383055"` |  |
| 16:49:43.207 | produce #66 | POST /produce | **200** | `"load-failover_grouped-65-1784546383172"` |  |
| 16:49:43.325 | produce #67 | POST /produce | **200** | `"load-failover_grouped-66-1784546383291"` |  |
| 16:49:43.457 | produce #68 | POST /produce | **200** | `"load-failover_grouped-67-1784546383411"` |  |
| 16:49:43.575 | produce #69 | POST /produce | **200** | `"load-failover_grouped-68-1784546383544"` |  |
| 16:49:43.698 | produce #70 | POST /produce | **200** | `"load-failover_grouped-69-1784546383666"` |  |
| 16:49:43.816 | produce #71 | POST /produce | **200** | `"load-failover_grouped-70-1784546383780"` |  |
| 16:49:43.930 | produce #72 | POST /produce | **200** | `"load-failover_grouped-71-1784546383899"` |  |
| 16:49:44.043 | produce #73 | POST /produce | **200** | `"load-failover_grouped-72-1784546384013"` |  |
| 16:49:44.174 | produce #74 | POST /produce | **200** | `"load-failover_grouped-73-1784546384138"` |  |
| 16:49:44.291 | produce #75 | POST /produce | **200** | `"load-failover_grouped-74-1784546384258"` |  |
| 16:49:44.421 | produce #76 | POST /produce | **200** | `"load-failover_grouped-75-1784546384389"` |  |
| 16:49:44.535 | produce #77 | POST /produce | **200** | `"load-failover_grouped-76-1784546384504"` |  |
| 16:49:44.652 | produce #78 | POST /produce | **200** | `"load-failover_grouped-77-1784546384621"` |  |
| 16:49:44.780 | produce #79 | POST /produce | **200** | `"load-failover_grouped-78-1784546384748"` |  |
| 16:49:44.896 | produce #80 | POST /produce | **200** | `"load-failover_grouped-79-1784546384864"` |  |
| 16:49:45.017 | produce #81 | POST /produce | **200** | `"load-failover_grouped-80-1784546384984"` |  |
| 16:49:45.134 | produce #82 | POST /produce | **200** | `"load-failover_grouped-81-1784546385101"` |  |
| 16:49:45.256 | produce #83 | POST /produce | **200** | `"load-failover_grouped-82-1784546385223"` |  |
| 16:49:45.376 | produce #84 | POST /produce | **200** | `"load-failover_grouped-83-1784546385341"` |  |
| 16:49:45.495 | produce #85 | POST /produce | **200** | `"load-failover_grouped-84-1784546385461"` |  |
| 16:49:45.610 | produce #86 | POST /produce | **200** | `"load-failover_grouped-85-1784546385579"` |  |
| 16:49:45.724 | produce #87 | POST /produce | **200** | `"load-failover_grouped-86-1784546385692"` |  |
| 16:49:45.839 | produce #88 | POST /produce | **200** | `"load-failover_grouped-87-1784546385809"` |  |
| 16:49:45.955 | produce #89 | POST /produce | **200** | `"load-failover_grouped-88-1784546385926"` |  |
| 16:49:46.070 | produce #90 | POST /produce | **200** | `"load-failover_grouped-89-1784546386040"` |  |
| 16:49:46.194 | produce #91 | POST /produce | **200** | `"load-failover_grouped-90-1784546386164"` |  |
| 16:49:46.308 | produce #92 | POST /produce | **200** | `"load-failover_grouped-91-1784546386279"` |  |
| 16:49:46.422 | produce #93 | POST /produce | **200** | `"load-failover_grouped-92-1784546386390"` |  |
| 16:49:46.555 | produce #94 | POST /produce | **200** | `"load-failover_grouped-93-1784546386524"` |  |
| 16:49:46.673 | produce #95 | POST /produce | **200** | `"load-failover_grouped-94-1784546386641"` |  |
| 16:49:46.791 | produce #96 | POST /produce | **200** | `"load-failover_grouped-95-1784546386759"` |  |
| 16:49:46.907 | produce #97 | POST /produce | **200** | `"load-failover_grouped-96-1784546386876"` |  |
| 16:49:47.024 | produce #98 | POST /produce | **200** | `"load-failover_grouped-97-1784546386992"` |  |
| 16:49:47.147 | produce #99 | POST /produce | **200** | `"load-failover_grouped-98-1784546387116"` |  |
| 16:49:47.265 | produce #100 | POST /produce | **200** | `"load-failover_grouped-99-1784546387233"` |  |
| 16:49:47.395 | produce #101 | POST /produce | **200** | `"load-failover_grouped-100-1784546387364"` |  |
| 16:49:47.515 | produce #102 | POST /produce | **200** | `"load-failover_grouped-101-1784546387484"` |  |
| 16:49:47.635 | produce #103 | POST /produce | **200** | `"load-failover_grouped-102-1784546387604"` |  |
| 16:49:47.753 | produce #104 | POST /produce | **200** | `"load-failover_grouped-103-1784546387717"` |  |
| 16:49:47.879 | produce #105 | POST /produce | **200** | `"load-failover_grouped-104-1784546387848"` |  |
| 16:49:47.999 | produce #106 | POST /produce | **200** | `"load-failover_grouped-105-1784546387965"` |  |
| 16:49:48.118 | produce #107 | POST /produce | **200** | `"load-failover_grouped-106-1784546388087"` |  |
| 16:49:48.236 | produce #108 | POST /produce | **200** | `"load-failover_grouped-107-1784546388201"` |  |
| 16:49:48.354 | produce #109 | POST /produce | **200** | `"load-failover_grouped-108-1784546388323"` |  |
| 16:49:48.471 | produce #110 | POST /produce | **200** | `"load-failover_grouped-109-1784546388440"` |  |
| 16:49:48.589 | produce #111 | POST /produce | **200** | `"load-failover_grouped-110-1784546388557"` |  |
| 16:49:48.756 | produce #112 | POST /produce | **200** | `"load-failover_grouped-111-1784546388677"` |  |
| 16:49:48.924 | produce #113 | POST /produce | **200** | `"load-failover_grouped-112-1784546388877"` |  |
| 16:49:49.048 | produce #114 | POST /produce | **200** | `"load-failover_grouped-113-1784546389016"` |  |
| 16:49:49.166 | produce #115 | POST /produce | **200** | `"load-failover_grouped-114-1784546389134"` |  |
| 16:49:49.285 | produce #116 | POST /produce | **200** | `"load-failover_grouped-115-1784546389251"` |  |
| 16:49:49.401 | produce #117 | POST /produce | **200** | `"load-failover_grouped-116-1784546389368"` |  |
| 16:49:49.554 | produce #118 | POST /produce | **200** | `"load-failover_grouped-117-1784546389487"` |  |
| 16:49:49.676 | produce #119 | POST /produce | **200** | `"load-failover_grouped-118-1784546389642"` |  |
| 16:49:49.795 | produce #120 | POST /produce | **200** | `"load-failover_grouped-119-1784546389761"` |  |
| 16:49:49.916 | produce #121 | POST /produce | **200** | `"load-failover_grouped-120-1784546389880"` |  |
| 16:49:50.030 | produce #122 | POST /produce | **200** | `"load-failover_grouped-121-1784546389999"` |  |
| 16:49:50.166 | produce #123 | POST /produce | **200** | `"load-failover_grouped-122-1784546390133"` |  |
| 16:49:50.281 | produce #124 | POST /produce | **200** | `"load-failover_grouped-123-1784546390252"` |  |
| 16:49:50.398 | produce #125 | POST /produce | **200** | `"load-failover_grouped-124-1784546390366"` |  |
| 16:49:50.515 | produce #126 | POST /produce | **200** | `"load-failover_grouped-125-1784546390482"` |  |
| 16:49:50.635 | produce #127 | POST /produce | **200** | `"load-failover_grouped-126-1784546390602"` |  |
| 16:49:50.751 | produce #128 | POST /produce | **200** | `"load-failover_grouped-127-1784546390720"` |  |
| 16:49:50.866 | produce #129 | POST /produce | **200** | `"load-failover_grouped-128-1784546390836"` |  |
| 16:49:50.984 | produce #130 | POST /produce | **200** | `"load-failover_grouped-129-1784546390952"` |  |
| 16:49:51.123 | produce #131 | POST /produce | **200** | `"load-failover_grouped-130-1784546391091"` |  |
| 16:49:51.238 | produce #132 | POST /produce | **200** | `"load-failover_grouped-131-1784546391208"` |  |
| 16:49:51.356 | produce #133 | POST /produce | **200** | `"load-failover_grouped-132-1784546391323"` |  |
| 16:49:51.472 | produce #134 | POST /produce | **200** | `"load-failover_grouped-133-1784546391442"` |  |
| 16:49:51.600 | produce #135 | POST /produce | **200** | `"load-failover_grouped-134-1784546391569"` |  |
| 16:49:51.713 | produce #136 | POST /produce | **200** | `"load-failover_grouped-135-1784546391682"` |  |
| 16:49:51.834 | produce #137 | POST /produce | **200** | `"load-failover_grouped-136-1784546391802"` |  |
| 16:49:51.945 | produce #138 | POST /produce | **200** | `"load-failover_grouped-137-1784546391915"` |  |
| 16:49:52.061 | produce #139 | POST /produce | **200** | `"load-failover_grouped-138-1784546392029"` |  |
| 16:49:52.175 | produce #140 | POST /produce | **200** | `"load-failover_grouped-139-1784546392145"` |  |
| 16:49:52.294 | produce #141 | POST /produce | **200** | `"load-failover_grouped-140-1784546392264"` |  |
| 16:49:52.431 | produce #142 | POST /produce | **200** | `"load-failover_grouped-141-1784546392400"` |  |
| 16:49:52.544 | produce #143 | POST /produce | **200** | `"load-failover_grouped-142-1784546392516"` |  |
| 16:49:52.687 | produce #144 | POST /produce | **200** | `"load-failover_grouped-143-1784546392654"` |  |
| 16:49:52.824 | produce #145 | POST /produce | **200** | `"load-failover_grouped-144-1784546392792"` |  |
| 16:49:52.947 | produce #146 | POST /produce | **200** | `"load-failover_grouped-145-1784546392910"` |  |
| 16:49:53.064 | produce #147 | POST /produce | **200** | `"load-failover_grouped-146-1784546393034"` |  |
| 16:49:53.179 | produce #148 | POST /produce | **200** | `"load-failover_grouped-147-1784546393149"` |  |
| 16:49:53.297 | produce #149 | POST /produce | **200** | `"load-failover_grouped-148-1784546393266"` |  |
| 16:49:53.416 | produce #150 | POST /produce | **200** | `"load-failover_grouped-149-1784546393382"` |  |
| 16:49:53.530 | produce #151 | POST /produce | **200** | `"load-failover_grouped-150-1784546393498"` |  |
| 16:49:53.653 | produce #152 | POST /produce | **200** | `"load-failover_grouped-151-1784546393620"` |  |
| 16:49:53.775 | produce #153 | POST /produce | **200** | `"load-failover_grouped-152-1784546393744"` |  |
| 16:49:53.892 | produce #154 | POST /produce | **200** | `"load-failover_grouped-153-1784546393860"` |  |
| 16:49:54.006 | produce #155 | POST /produce | **200** | `"load-failover_grouped-154-1784546393975"` |  |
| 16:49:54.126 | produce #156 | POST /produce | **200** | `"load-failover_grouped-155-1784546394092"` |  |
| 16:49:54.245 | produce #157 | POST /produce | **200** | `"load-failover_grouped-156-1784546394214"` |  |
| 16:49:54.357 | produce #158 | POST /produce | **200** | `"load-failover_grouped-157-1784546394328"` |  |
| 16:49:54.490 | produce #159 | POST /produce | **200** | `"load-failover_grouped-158-1784546394457"` |  |
| 16:49:54.607 | produce #160 | POST /produce | **200** | `"load-failover_grouped-159-1784546394576"` |  |
| 16:49:54.754 | produce #161 | POST /produce | **200** | `"load-failover_grouped-160-1784546394718"` |  |
| 16:49:54.873 | produce #162 | POST /produce | **200** | `"load-failover_grouped-161-1784546394842"` |  |
| 16:49:54.991 | produce #163 | POST /produce | **200** | `"load-failover_grouped-162-1784546394960"` |  |
| 16:49:55.110 | produce #164 | POST /produce | **200** | `"load-failover_grouped-163-1784546395077"` |  |
| 16:49:55.245 | produce #165 | POST /produce | **200** | `"load-failover_grouped-164-1784546395214"` |  |
| 16:49:55.365 | produce #166 | POST /produce | **200** | `"load-failover_grouped-165-1784546395331"` |  |
| 16:49:55.485 | produce #167 | POST /produce | **200** | `"load-failover_grouped-166-1784546395450"` |  |
| 16:49:55.599 | produce #168 | POST /produce | **200** | `"load-failover_grouped-167-1784546395567"` |  |
| 16:49:55.760 | produce #169 | POST /produce | **200** | `"load-failover_grouped-168-1784546395685"` |  |
| 16:49:55.878 | produce #170 | POST /produce | **200** | `"load-failover_grouped-169-1784546395847"` |  |
| 16:49:55.994 | produce #171 | POST /produce | **200** | `"load-failover_grouped-170-1784546395963"` |  |
| 16:49:56.122 | produce #172 | POST /produce | **200** | `"load-failover_grouped-171-1784546396090"` |  |
| 16:49:56.236 | produce #173 | POST /produce | **200** | `"load-failover_grouped-172-1784546396206"` |  |
| 16:49:56.376 | produce #174 | POST /produce | **200** | `"load-failover_grouped-173-1784546396343"` |  |
| 16:49:56.503 | produce #175 | POST /produce | **200** | `"load-failover_grouped-174-1784546396471"` |  |
| 16:49:56.638 | produce #176 | POST /produce | **200** | `"load-failover_grouped-175-1784546396606"` |  |
| 16:49:56.752 | produce #177 | POST /produce | **200** | `"load-failover_grouped-176-1784546396722"` |  |
| 16:49:56.894 | produce #178 | POST /produce | **200** | `"load-failover_grouped-177-1784546396863"` |  |
| 16:49:57.014 | produce #179 | POST /produce | **200** | `"load-failover_grouped-178-1784546396979"` |  |
| 16:49:57.126 | produce #180 | POST /produce | **200** | `"load-failover_grouped-179-1784546397096"` |  |
| 16:49:57.241 | produce #181 | POST /produce | **200** | `"load-failover_grouped-180-1784546397211"` |  |
| 16:49:57.354 | produce #182 | POST /produce | **200** | `"load-failover_grouped-181-1784546397323"` |  |
| 16:49:57.502 | produce #183 | POST /produce | **200** | `"load-failover_grouped-182-1784546397468"` |  |
| 16:49:57.623 | produce #184 | POST /produce | **200** | `"load-failover_grouped-183-1784546397589"` |  |
| 16:49:57.742 | produce #185 | POST /produce | **200** | `"load-failover_grouped-184-1784546397713"` |  |
| 16:49:57.856 | produce #186 | POST /produce | **200** | `"load-failover_grouped-185-1784546397825"` |  |
| 16:49:57.968 | produce #187 | POST /produce | **200** | `"load-failover_grouped-186-1784546397938"` |  |
| 16:49:58.090 | produce #188 | POST /produce | **200** | `"load-failover_grouped-187-1784546398059"` |  |
| 16:49:58.206 | produce #189 | POST /produce | **200** | `"load-failover_grouped-188-1784546398175"` |  |
| 16:49:58.347 | produce #190 | POST /produce | **200** | `"load-failover_grouped-189-1784546398312"` |  |
| 16:49:58.465 | produce #191 | POST /produce | **200** | `"load-failover_grouped-190-1784546398432"` |  |
| 16:49:58.584 | produce #192 | POST /produce | **200** | `"load-failover_grouped-191-1784546398552"` |  |
| 16:49:58.712 | produce #193 | POST /produce | **200** | `"load-failover_grouped-192-1784546398682"` |  |
| 16:49:58.839 | produce #194 | POST /produce | **200** | `"load-failover_grouped-193-1784546398805"` |  |
| 16:49:58.991 | produce #195 | POST /produce | **200** | `"load-failover_grouped-194-1784546398960"` |  |
| 16:49:59.110 | produce #196 | POST /produce | **200** | `"load-failover_grouped-195-1784546399079"` |  |
| 16:49:59.227 | produce #197 | POST /produce | **200** | `"load-failover_grouped-196-1784546399195"` |  |
| 16:49:59.350 | produce #198 | POST /produce | **200** | `"load-failover_grouped-197-1784546399317"` |  |
| 16:49:59.480 | produce #199 | POST /produce | **200** | `"load-failover_grouped-198-1784546399447"` |  |
| 16:49:59.610 | produce #200 | POST /produce | **200** | `"load-failover_grouped-199-1784546399578"` |  |
| 16:49:59.728 | produce #201 | POST /produce | **200** | `"load-failover_grouped-200-1784546399696"` |  |
| 16:49:59.856 | produce #202 | POST /produce | **200** | `"load-failover_grouped-201-1784546399824"` |  |
| 16:49:59.995 | produce #203 | POST /produce | **200** | `"load-failover_grouped-202-1784546399958"` |  |
| 16:50:00.117 | produce #204 | POST /produce | **200** | `"load-failover_grouped-203-1784546400082"` |  |
| 16:50:00.236 | produce #205 | POST /produce | **200** | `"load-failover_grouped-204-1784546400202"` |  |
| 16:50:00.358 | produce #206 | POST /produce | **200** | `"load-failover_grouped-205-1784546400321"` |  |
| 16:50:00.490 | produce #207 | POST /produce | **200** | `"load-failover_grouped-206-1784546400456"` |  |
| 16:50:00.610 | produce #208 | POST /produce | **200** | `"load-failover_grouped-207-1784546400580"` |  |
| 16:50:00.743 | produce #209 | POST /produce | **200** | `"load-failover_grouped-208-1784546400713"` |  |
| 16:50:00.860 | produce #210 | POST /produce | **200** | `"load-failover_grouped-209-1784546400829"` |  |
| 16:50:00.974 | produce #211 | POST /produce | **200** | `"load-failover_grouped-210-1784546400943"` |  |
| 16:50:01.091 | produce #212 | POST /produce | **200** | `"load-failover_grouped-211-1784546401057"` |  |
| 16:50:01.205 | produce #213 | POST /produce | **200** | `"load-failover_grouped-212-1784546401174"` |  |
| 16:50:01.323 | produce #214 | POST /produce | **200** | `"load-failover_grouped-213-1784546401290"` |  |
| 16:50:01.440 | produce #215 | POST /produce | **200** | `"load-failover_grouped-214-1784546401410"` |  |
| 16:50:01.556 | produce #216 | POST /produce | **200** | `"load-failover_grouped-215-1784546401525"` |  |
| 16:50:01.692 | produce #217 | POST /produce | **200** | `"load-failover_grouped-216-1784546401660"` |  |
| 16:50:01.808 | produce #218 | POST /produce | **200** | `"load-failover_grouped-217-1784546401775"` |  |
| 16:50:01.926 | produce #219 | POST /produce | **200** | `"load-failover_grouped-218-1784546401895"` |  |
| 16:50:02.043 | produce #220 | POST /produce | **200** | `"load-failover_grouped-219-1784546402014"` |  |
| 16:50:02.158 | produce #221 | POST /produce | **200** | `"load-failover_grouped-220-1784546402127"` |  |
| 16:50:02.282 | produce #222 | POST /produce | **200** | `"load-failover_grouped-221-1784546402252"` |  |
| 16:50:02.397 | produce #223 | POST /produce | **200** | `"load-failover_grouped-222-1784546402368"` |  |
| 16:50:02.540 | produce #224 | POST /produce | **200** | `"load-failover_grouped-223-1784546402508"` |  |
| 16:50:02.669 | produce #225 | POST /produce | **200** | `"load-failover_grouped-224-1784546402635"` |  |
| 16:50:02.797 | produce #226 | POST /produce | **200** | `"load-failover_grouped-225-1784546402755"` |  |
| 16:50:02.922 | produce #227 | POST /produce | **200** | `"load-failover_grouped-226-1784546402879"` |  |
| 16:50:03.177 | produce #228 | POST /produce | **200** | `"load-failover_grouped-227-1784546403023"` |  |
| 16:50:03.342 | produce #229 | POST /produce | **200** | `"load-failover_grouped-228-1784546403304"` |  |
| 16:50:03.457 | produce #230 | POST /produce | **200** | `"load-failover_grouped-229-1784546403425"` |  |
| 16:50:03.580 | produce #231 | POST /produce | **200** | `"load-failover_grouped-230-1784546403547"` |  |
| 16:50:03.701 | produce #232 | POST /produce | **200** | `"load-failover_grouped-231-1784546403667"` |  |
| 16:50:03.815 | produce #233 | POST /produce | **200** | `"load-failover_grouped-232-1784546403785"` |  |
| 16:50:03.941 | produce #234 | POST /produce | **200** | `"load-failover_grouped-233-1784546403899"` |  |
| 16:50:04.140 | produce #235 | POST /produce | **200** | `"load-failover_grouped-234-1784546404097"` |  |
| 16:50:04.261 | produce #236 | POST /produce | **200** | `"load-failover_grouped-235-1784546404230"` |  |
| 16:50:04.381 | produce #237 | POST /produce | **200** | `"load-failover_grouped-236-1784546404348"` |  |
| 16:50:04.497 | produce #238 | POST /produce | **200** | `"load-failover_grouped-237-1784546404465"` |  |
| 16:50:04.626 | produce #239 | POST /produce | **200** | `"load-failover_grouped-238-1784546404592"` |  |
| 16:50:04.791 | produce #240 | POST /produce | **200** | `"load-failover_grouped-239-1784546404759"` |  |
| 16:50:04.961 | produce #241 | POST /produce | **200** | `"load-failover_grouped-240-1784546404887"` |  |
| 16:50:05.097 | produce #242 | POST /produce | **200** | `"load-failover_grouped-241-1784546405053"` |  |
| 16:50:05.211 | produce #243 | POST /produce | **200** | `"load-failover_grouped-242-1784546405180"` |  |
| 16:50:05.335 | produce #244 | POST /produce | **200** | `"load-failover_grouped-243-1784546405300"` |  |
| 16:50:05.458 | produce #245 | POST /produce | **200** | `"load-failover_grouped-244-1784546405423"` |  |
| 16:50:05.583 | produce #246 | POST /produce | **200** | `"load-failover_grouped-245-1784546405548"` |  |
| 16:50:05.704 | produce #247 | POST /produce | **200** | `"load-failover_grouped-246-1784546405666"` |  |
| 16:50:05.825 | produce #248 | POST /produce | **200** | `"load-failover_grouped-247-1784546405791"` |  |
| 16:50:05.938 | produce #249 | POST /produce | **200** | `"load-failover_grouped-248-1784546405907"` |  |
| 16:50:06.052 | produce #250 | POST /produce | **200** | `"load-failover_grouped-249-1784546406022"` |  |
| 16:50:06.165 | produce #251 | POST /produce | **200** | `"load-failover_grouped-250-1784546406135"` |  |
| 16:50:06.281 | produce #252 | POST /produce | **200** | `"load-failover_grouped-251-1784546406250"` |  |
| 16:50:06.407 | produce #253 | POST /produce | **200** | `"load-failover_grouped-252-1784546406369"` |  |
| 16:50:06.524 | produce #254 | POST /produce | **200** | `"load-failover_grouped-253-1784546406493"` |  |
| 16:50:06.646 | produce #255 | POST /produce | **200** | `"load-failover_grouped-254-1784546406610"` |  |
| 16:50:06.776 | produce #256 | POST /produce | **200** | `"load-failover_grouped-255-1784546406739"` |  |
| 16:50:06.894 | produce #257 | POST /produce | **200** | `"load-failover_grouped-256-1784546406861"` |  |
| 16:50:07.008 | produce #258 | POST /produce | **200** | `"load-failover_grouped-257-1784546406977"` |  |
| 16:50:07.127 | produce #259 | POST /produce | **200** | `"load-failover_grouped-258-1784546407095"` |  |
| 16:50:07.249 | produce #260 | POST /produce | **200** | `"load-failover_grouped-259-1784546407217"` |  |
| 16:50:07.364 | produce #261 | POST /produce | **200** | `"load-failover_grouped-260-1784546407334"` |  |
| 16:50:07.479 | produce #262 | POST /produce | **200** | `"load-failover_grouped-261-1784546407448"` |  |
| 16:50:07.602 | produce #263 | POST /produce | **200** | `"load-failover_grouped-262-1784546407569"` |  |
| 16:50:07.723 | produce #264 | POST /produce | **200** | `"load-failover_grouped-263-1784546407691"` |  |
| 16:50:07.841 | produce #265 | POST /produce | **200** | `"load-failover_grouped-264-1784546407811"` |  |
| 16:50:07.972 | produce #266 | POST /produce | **200** | `"load-failover_grouped-265-1784546407933"` |  |
| 16:50:08.085 | produce #267 | POST /produce | **200** | `"load-failover_grouped-266-1784546408055"` |  |
| 16:50:08.198 | produce #268 | POST /produce | **200** | `"load-failover_grouped-267-1784546408167"` |  |
| 16:50:08.316 | produce #269 | POST /produce | **200** | `"load-failover_grouped-268-1784546408285"` |  |
| 16:50:08.467 | produce #270 | POST /produce | **200** | `"load-failover_grouped-269-1784546408436"` |  |
| 16:50:08.584 | produce #271 | POST /produce | **200** | `"load-failover_grouped-270-1784546408554"` |  |
| 16:50:08.701 | produce #272 | POST /produce | **200** | `"load-failover_grouped-271-1784546408670"` |  |
| 16:50:08.813 | produce #273 | POST /produce | **200** | `"load-failover_grouped-272-1784546408783"` |  |
| 16:50:08.950 | produce #274 | POST /produce | **200** | `"load-failover_grouped-273-1784546408917"` |  |
| 16:50:09.078 | produce #275 | POST /produce | **200** | `"load-failover_grouped-274-1784546409047"` |  |
| 16:50:09.192 | produce #276 | POST /produce | **200** | `"load-failover_grouped-275-1784546409159"` |  |
| 16:50:09.309 | produce #277 | POST /produce | **200** | `"load-failover_grouped-276-1784546409275"` |  |
| 16:50:09.421 | produce #278 | POST /produce | **200** | `"load-failover_grouped-277-1784546409390"` |  |
| 16:50:09.534 | produce #279 | POST /produce | **200** | `"load-failover_grouped-278-1784546409504"` |  |
| 16:50:09.648 | produce #280 | POST /produce | **200** | `"load-failover_grouped-279-1784546409617"` |  |
| 16:50:09.760 | produce #281 | POST /produce | **200** | `"load-failover_grouped-280-1784546409730"` |  |
| 16:50:09.878 | produce #282 | POST /produce | **200** | `"load-failover_grouped-281-1784546409848"` |  |
| 16:50:09.996 | produce #283 | POST /produce | **200** | `"load-failover_grouped-282-1784546409967"` |  |
| 16:50:10.132 | produce #284 | POST /produce | **200** | `"load-failover_grouped-283-1784546410082"` |  |
| 16:50:10.311 | produce #285 | POST /produce | **200** | `"load-failover_grouped-284-1784546410277"` |  |
| 16:50:10.428 | produce #286 | POST /produce | **200** | `"load-failover_grouped-285-1784546410394"` |  |
| 16:50:10.545 | produce #287 | POST /produce | **200** | `"load-failover_grouped-286-1784546410515"` |  |
| 16:50:10.675 | produce #288 | POST /produce | **200** | `"load-failover_grouped-287-1784546410631"` |  |
| 16:50:10.799 | produce #289 | POST /produce | **200** | `"load-failover_grouped-288-1784546410762"` |  |
| 16:50:10.909 | GET failover (idle) | GET /failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_pr...` | Transition deleted |
| 16:50:10.921 | produce #290 | POST /produce | **200** | `"load-failover_grouped-289-1784546410890"` |  |
| 16:50:11.012 | GET failover (idle) | GET /failover | **500** | `{"reason":"java.lang.Exception: com.flipkart.varadhi.common.exceptions.ResourceNotFoundException: No active failover for topic default_pr...` | Transition deleted |
| 16:50:11.127 | produce #291 | POST /produce | **200** | `"load-failover_grouped-290-1784546411021"` |  |

### SWITCH GET response (16:49:33.820)

```json
{
  "topicFqn": "default_project.failover_grouped",
  "version": 2,
  "transitionKind": "FAILOVER",
  "operationId": "ad03d757-913c-4587-b28e-57c2dbb5d1b5",
  "sourceRegion": "region-a",
  "targetRegion": "region-b",
  "createdAt": 1784546373568,
  "currentStage": "SWITCH",
  "topicVersionToAwait": 33,
  "updatedAt": 1784546373768,
  "stageHistory": [
    {
      "stage": "PREPARE",
      "startedAt": 1784546373648,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    },
    {
      "stage": "SWITCH",
      "startedAt": 1784546373768,
      "ackedHosts": [],
      "endedAt": 0,
      "outcome": "IN_PROGRESS"
    }
  ],
  "name": "default_project.failover_grouped",
  "entityType": "TRANSITION_OBJECT"
}
```

## ZK topic snapshots during failover

| Time | Phase | version | topicState | producing | Snapshot |
|------|-------|---------|------------|-----------|----------|
| 16:49:32.814 | before failover | 31 | `Producing` | `region-a` | see below |
| 16:49:33.898 | SWITCH (Fenced) | 32 | `Fenced` | `region-b` | see below |

### ZK topic @ before failover (16:49:32.814)

```json
{
  "topicState": "Producing",
  "version": 31,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {
    "region-a": {
      "produceAllowed": true
    },
    "region-b": {
      "produceAllowed": false
    }
  },
  "_zkVersion": 32
}
```

### ZK topic @ SWITCH (Fenced) (16:49:33.898)

```json
{
  "topicState": "Fenced",
  "version": 32,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {
    "region-a": {
      "produceAllowed": false
    },
    "region-b": {
      "produceAllowed": true
    }
  },
  "_zkVersion": 33
}
```

## ZK topic after failover

```json
{
  "topicState": "Fenced",
  "version": 32,
  "grouped": true,
  "autoFailover": false,
  "regionConfigs": {
    "region-a": {
      "produceAllowed": false
    },
    "region-b": {
      "produceAllowed": true
    }
  },
  "_zkVersion": 33
}
```

## Produce HTTP summary

| HTTP | Count | Phase |
|------|-------|-------|
| 200 | 290 | PREPARE + post-COMPLETED |
| 422 | 0 | SWITCH fenced window |
