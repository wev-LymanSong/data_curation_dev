
we_mart.wv_server_log_base
==========================

# BASIC INFO

|**About**| 담당자 수기 입력 필요 |
| :--- | :--- |
|**Database**|**we_mart**|
|**Table Type**|MART SECONDARY|
|**Partitioned by**|`date_id`|
|**Created/ Last Updated At**|2022-09-06 / 2024-02-23|
|**Created By**|박상민|
|**Last Updated By**|박상민|
|**Collaborators**|박상민[8], 구민서[1]|
  
#### Change History
|**Date**|**By**|**LINK**|
| :--- | :--- | :--- |
|2022-09-06|박상민|[PR](https://github.com/benxcorp/databricks/commit/83035cd5bae4e36442bafe06c5e0497d08452aed)|
|2022-09-16|박상민|[PR](https://github.com/benxcorp/databricks/commit/2d77f9ef72e5905a7ffcf8436869fa3c9f9d5a5d)|
|2022-10-05|박상민|[PR](https://github.com/benxcorp/databricks/commit/48024a18333a9cba0522c2178a7e78d202a3ae4d)|
|2022-10-05|박상민|[PR](https://github.com/benxcorp/databricks/commit/39ac9488dc148cdac1d6dcfe28915a712ee2978f)|
|2022-10-17|박상민|[PR](https://github.com/benxcorp/databricks/commit/883c55a2a468b57368d0300931c02a0fa3e3f176)|
|2022-11-01|박상민|[PR](https://github.com/benxcorp/databricks/commit/4d1557f46e47fb858f7e4ee2e9288c42d9ad0ae7)|
|2022-11-21|박상민|[PR](https://github.com/benxcorp/databricks/commit/83c77e9bab478f05eb6fcce1d18229b7d18c18d2)|
|2022-12-09|구민서|[PR](https://github.com/benxcorp/databricks/commit/bd183779c84e616084e05627330a1c8b98ca44bd)|
|2024-02-23|박상민|[PR](https://github.com/benxcorp/databricks/commit/d4e6bab6460f72a96687b53b9adfd810e0f5da2a)|
  
  
# TABLE NOTICE
  
### 테이블 개요

* **테이블 목적**: Weverse 서버 로그 데이터를 전처리하여 사용자 세션, 접속 정보, 로그 정보, URL 정보, API 호출 정보 등을 담은 분석용 테이블
* **데이터 레벨**: TRANSACTIONAL DATA
* **파티션 키**: `date_id`
* **주요 키**: `user_sess_id`

### 테이블 특징

* `user_sess_id` 컬럼은 `sess_id`와 `user_info_id`를 조합하여 사용자 세션을 식별
* 사용자 세션 시작/종료 시간, 플랫폼, OS, 앱 버전, URL, API 호출 정보 등을 포함
* `is_join`, `is_leave` 컬럼을 통해 사용자의 커뮤니티 가입/탈퇴 여부 확인 가능
* `chat_msg_count`, `chat_msg_list` 컬럼을 통해 채팅 메시지 수, 채팅 메시지 리스트 정보 확인 가능

### 데이터 추출 및 생성 과정

1. **주요 데이터 소스**:
    * `service_log.weverse_server_log`: Weverse 서버 로그 데이터
2. **데이터 전처리**:
    * `unix_log_timestamp` 컬럼을 `timestamp` 형식으로 변환
    * `params` 컬럼에서 `_request_body`, `_method`, `_gcc`, `_consumer_id`, `_provider_id` 값 추출
    * 사용자 에이전트에서 앱 버전 정보 추출
    * URL에서 커뮤니티 ID, 포스트 ID, 알림 ID, 채팅 ID 추출
    * `request` 컬럼에서 추가 정보 추출
3. **데이터 통합**:
    * `user_info_device_id` 컬럼을 기준으로 사용자 세션 정보 통합
    * 사용자 세션 시작/종료 시간, 세션 시퀀스 정보 계산
    * `user_info_id` 컬럼을 기준으로 사용자 세션 정보 통합
4. **최종 테이블 생성**:
    * 위 과정을 거쳐 전처리된 데이터를 `we_mart.wv_server_log_base` 테이블에 저장

### 테이블 활용 가이드

* **주요 활용**:
    * Weverse 서버 로그 데이터 분석
    * 사용자 세션 정보 분석
    * API 호출 정보 분석
    * 사용자 행동 패턴 분석
* **조인 시 유의사항**:
    * `user_info_id`, `user_info_device_id`, `sess_id` 컬럼을 이용하여 다른 테이블과 조인 가능
    * `date_id` 컬럼을 이용하여 다른 테이블과 파티션 단위로 조인 가능

### 추가 정보

* `we_mart.wv_server_log_base` 테이블은 Weverse 서버 로그 데이터를 전처리한 결과를 담고 있으므로, 원본 로그 데이터와 일치하지 않을 수 있음
* `user_sess_id` 컬럼은 사용자 세션을 식별하는 주요 키이므로, 다른 테이블과 조인할 때 유용하게 활용 가능
* `is_join`, `is_leave` 컬럼을 통해 사용자의 커뮤니티 가입/탈퇴 행동 분석 가능
* `chat_msg_count`, `chat_msg_list` 컬럼을 통해 채팅 활동 분석 가능  
---
# COLUMN INFO

|#|Column Name|Data Type|Comment|
| :--- | :--- | :--- | :--- |
|0|date_id|string|date_id(KST)|
|1|hour|string|hour(KST)|
|2|log_dt|timestamp|로그 produce_time (UTC)|
|3|user_info_user_key|string| |
|4|sess_id|string|로그시작일시(yyyyMMddHHmmssSSS)\|user_info_device_id|
|5|sess_seq|bigint||
|6|sess_start_dt|timestamp|세션id 최초일시(UTC)|
|7|sess_end_dt|timestamp|세션id 최종일시(UTC)|
|8|user_sess_id|string|session_id + user_id|
|9|user_sess_start_dt|timestamp|user_sess_id 시작일시|
|10|user_sess_end_dt|timestamp|user_sess_id 종료일시|
|11|user_info_id|string| |
|12|user_info_device_id|string| |
|13|platform|string|접속 플랫폼|
|14|os|string|접속 OS|
|15|app_ver|string|APP ver(user_agent)|
|16|user_id_fill|string|비로그인 상태 user_id 추가|
|17|user_info_status|string| |
|18|user_info_ip_address|string| |
|19|user_info_locale|string| |
|20|user_info_country|string| |
|21|topic|string|parmas|
|22|method|string|parmas|
|23|gcc|string|parmas|
|24|language|string|parmas|
|25|consumer_id|string|parmas|
|26|provider_id|string|parmas|
|27|platform_id|string|parmas|
|28|wpf|string|parmas|
|29|is_join|int|url|
|30|is_leave|int|url|
|31|post_id|string|nvl(url, params)|
|32|comm_id|string|nvl(url, params)|
|33|notice_id|string|url|
|34|body|string|parmas|
|35|media_type|string|parmas|
|36|media_time|string|parmas|
|37|video_id|string|parmas|
|38|video_session_id|string|parmas|
|39|section_type|string|parmas|
|40|is_product|string|parmas|
|41|is_hide_from_artist|string|parmas|
|42|is_fc_only|string|parmas|
|43|chat_id|string|params|
|44|chat_msg_count|int|params|
|45|chat_msg_list|string|params|
|46|url|string| |
|47|params|string| |
|48|user_info_user_agent|string| |
|49|run_timestamp|timestamp|배치일시(UTC)|
  
    
---
# HOW TO USE
  
### Downstream Table/View
- `wv_server_log_base` 테이블을 사용하여 `we_art_id` 기준으로 `date_id` 컬럼을 파티션 키로 사용하는 `wv_server_log_daily` 테이블을 생성
    - ```sql
      create table we_mart.wv_server_log_daily
      (
      date_id	string	comment "date_id(KST)"
      , hour	string	comment "hour(KST)"
      , log_dt	timestamp	comment "로그 produce_time (UTC)"
      , user_info_user_key	string	
      , sess_id	string	comment "로그시작일시(yyyyMMddHHmmssSSS)|user_info_device_id"
      , sess_seq	bigint	comment ""
      , sess_start_dt	timestamp	comment "세션id 최초일시(UTC)"
      , sess_end_dt	timestamp	comment "세션id 최종일시(UTC)"
      , user_info_id	string	
      , user_info_device_id	string	
      , platform	string	comment "접속 플랫폼"
      , os	string	comment "접속 OS"
      , app_ver	string	comment "APP ver(user_agent)"
      , user_info_status	string	
      , user_info_ip_address	string	
      , user_info_locale	string	
      , user_info_country	string	
      , topic	string	comment "parmas"
      , method	string	comment "parmas"
      , gcc	string	comment "parmas"
      , language	string	comment "parmas"
      , consumer_id	string	comment "parmas"
      , provider_id	string	comment "parmas"
      , platform_id	string	comment "parmas"
      , wpf	string	comment "parmas"
      , is_join	int	comment "url"
      , is_leave	int	comment "url"
      , post_id	string	comment "nvl(url, params)"
      , comm_id	string	comment "nvl(url, params)"
      , notice_id	string	comment "url"
      , body	string	comment "parmas"
      , media_type	string	comment "parmas"
      , media_time	string	comment "parmas"
      , video_id	string	comment "parmas"
      , video_session_id	string	comment "parmas"
      , section_type	string	comment "parmas"
      , is_product	string	comment "parmas"
      , is_hide_from_artist	string	comment "parmas"
      , is_fc_only	string	comment "parmas"
      , url	string	
      , params	string	
      , user_info_user_agent	string	
      , run_timestamp	timestamp	comment "배치일시(UTC)"
      ) 
      partitioned by (date_id)
      comment "WV 서버로그 전처리"
      as
      select * from we_mart.wv_server_log_base
      where we_art_id = "ARTIST";
      ```
- `wv_server_log_base` 테이블을 사용하여 `user_info_device_id` 컬럼을 파티션 키로 사용하는 `wv_server_log_device` 뷰를 생성
    - ```sql
      create or replace view we_mart.wv_server_log_device as
      select * from we_mart.wv_server_log_base
      where user_info_device_id = "DEVICE";
      ```
- `wv_server_log_base` 테이블을 사용하여 `user_info_id` 컬럼을 파티션 키로 사용하는 `wv_server_log_user` 테이블을 생성
    - ```sql
      create table we_mart.wv_server_log_user
      (
      date_id	string	comment "date_id(KST)"
      , hour	string	comment "hour(KST)"
      , log_dt	timestamp	comment "로그 produce_time (UTC)"
      , user_info_user_key	string	
      , sess_id	string	comment "로그시작일시(yyyyMMddHHmmssSSS)|user_info_device_id"
      , sess_seq	bigint	comment ""
      , sess_start_dt	timestamp	comment "세션id 최초일시(UTC)"
      , sess_end_dt	timestamp	comment "세션id 최종일시(UTC)"
      , user_info_id	string	
      , user_info_device_id	string	
      , platform	string	comment "접속 플랫폼"
      , os	string	comment "접속 OS"
      , app_ver	string	comment "APP ver(user_agent)"
      , user_info_status	string	
      , user_info_ip_address	string	
      , user_info_locale	string	
      , user_info_country	string	
      , topic	string	comment "parmas"
      , method	string	comment "parmas"
      , gcc	string	comment "parmas"
      , language	string	comment "parmas"
      , consumer_id	string	comment "parmas"
      , provider_id	string	comment "parmas"
      , platform_id	string	comment "parmas"
      , wpf	string	comment "parmas"
      , is_join	int	comment "url"
      , is_leave	int	comment "url"
      , post_id	string	comment "nvl(url, params)"
      , comm_id	string	comment "nvl(url, params)"
      , notice_id	string	comment "url"
      , body	string	comment "parmas"
      , media_type	string	comment "parmas"
      , media_time	string	comment "parmas"
      , video_id	string	comment "parmas"
      , video_session_id	string	comment "parmas"
      , section_type	string	comment "parmas"
      , is_product	string	comment "parmas"
      , is_hide_from_artist	string	comment "parmas"
      , is_fc_only	string	comment "parmas"
      , url	string	
      , params	string	
      , user_info_user_agent	string	
      , run_timestamp	timestamp	comment "배치일시(UTC)"
      ) 
      partitioned by (user_info_id)
      comment "WV 서버로그 전처리"
      as
      select * from we_mart.wv_server_log_base
      where user_info_id = "USER";
      ```
- `wv_server_log_base` 테이블과 `we_mart.we_artist` 테이블을 `comm_id` 컬럼을 기준으로 조인하여 `artist_server_log` 뷰를 생성
    - ```sql
      create or replace view we_mart.artist_server_log as
      select sl.*, a.*
      from we_mart.wv_server_log_base sl
      join we_mart.we_artist a on sl.comm_id = a.comm_id
      where a.we_art_id = "ARTIST";
      ```

### Data Extraction
- `2024-08-20` 날짜의 `post_id`가 "0-146093045"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and post_id = "0-146093045";
      ```
- `2024-08-20` 날짜의 `url`에 "/join"이 포함된 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and url rlike "/join";
      ```
- `2024-08-20` 날짜의 `user_info_device_id`가 "DEVICE"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and user_info_device_id = "DEVICE";
      ```
- `2024-08-20` 날짜의 `user_info_id`가 "USER"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and user_info_id = "USER";
      ```
- `2024-08-20` 날짜의 `comm_id`가 "COMMUNITY"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and comm_id = "COMMUNITY";
      ```
- `2024-08-20` 날짜의 `platform`이 "APP"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and platform = "APP";
      ```
- `2024-08-20` 날짜의 `os`가 "ANDROID"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and os = "ANDROID";
      ```
- `2024-08-20` 날짜의 `app_ver`가 "1.0.0"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and app_ver = "1.0.0";
      ```
- `2024-08-20` 날짜의 `user_info_status`가 "ACTIVE"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and user_info_status = "ACTIVE";
      ```
- `2024-08-20` 날짜의 `user_info_ip_address`가 "IP"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and user_info_ip_address = "IP";
      ```
- `2024-08-20` 날짜의 `user_info_locale`가 "ko-KR"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and user_info_locale = "ko-KR";
      ```
- `2024-08-20` 날짜의 `user_info_country`가 "KR"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and user_info_country = "KR";
      ```
- `2024-08-20` 날짜의 `topic`이 "ARTIST"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and topic = "ARTIST";
      ```
- `2024-08-20` 날짜의 `method`가 "GET"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and method = "GET";
      ```
- `2024-08-20` 날짜의 `gcc`가 "KR"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and gcc = "KR";
      ```
- `2024-08-20` 날짜의 `language`가 "ko"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and language = "ko";
      ```
- `2024-08-20` 날짜의 `consumer_id`가 "WEVAPP"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and consumer_id = "WEVAPP";
      ```
- `2024-08-20` 날짜의 `provider_id`가 "WEVAPP"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and provider_id = "WEVAPP";
      ```
- `2024-08-20` 날짜의 `platform_id`가 "ANDROID"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and platform_id = "ANDROID";
      ```
- `2024-08-20` 날짜의 `wpf`가 "MWEB"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and wpf = "MWEB";
      ```
- `2024-08-20` 날짜의 `is_join`이 1인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and is_join = 1;
      ```
- `2024-08-20` 날짜의 `is_leave`가 1인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and is_leave = 1;
      ```
- `2024-08-20` 날짜의 `post_id`가 "POST"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and post_id = "POST";
      ```
- `2024-08-20` 날짜의 `comm_id`가 "COMMUNITY"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and comm_id = "COMMUNITY";
      ```
- `2024-08-20` 날짜의 `notice_id`가 "NOTICE"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and notice_id = "NOTICE";
      ```
- `2024-08-20` 날짜의 `body`가 "BODY"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and body = "BODY";
      ```
- `2024-08-20` 날짜의 `media_type`이 "VIDEO"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and media_type = "VIDEO";
      ```
- `2024-08-20` 날짜의 `media_time`이 "TIME"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and media_time = "TIME";
      ```
- `2024-08-20` 날짜의 `video_id`가 "VIDEO"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and video_id = "VIDEO";
      ```
- `2024-08-20` 날짜의 `video_session_id`가 "SESSION"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and video_session_id = "SESSION";
      ```
- `2024-08-20` 날짜의 `section_type`이 "SECTION"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and section_type = "SECTION";
      ```
- `2024-08-20` 날짜의 `is_product`가 "TRUE"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and is_product = "TRUE";
      ```
- `2024-08-20` 날짜의 `is_hide_from_artist`가 "TRUE"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and is_hide_from_artist = "TRUE";
      ```
- `2024-08-20` 날짜의 `is_fc_only`가 "TRUE"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and is_fc_only = "TRUE";
      ```
- `2024-08-20` 날짜의 `url`이 "URL"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and url = "URL";
      ```
- `2024-08-20` 날짜의 `params`가 "PARAMS"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and params = "PARAMS";
      ```
- `2024-08-20` 날짜의 `user_info_user_agent`가 "USER_AGENT"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and user_info_user_agent = "USER_AGENT";
      ```
- `2024-08-20` 날짜의 `run_timestamp`가 "TIMESTAMP"인 로그 데이터 추출
    - ```sql
      select *
      from wev_prod.we_mart.wv_server_log_base
      where date_id = '2024-08-20'
      and run_timestamp = "TIMESTAMP";
      ```  
---
# PIPELINE INFO

## ⌛️ BATCH

### DAG: `analytics_we_mart_wv1_daily`, `analytics_log_daily`

### Update Interval: DAILY

### Update Type: OVERWRITE

## 📍 LINK URLs

### Github: [Source Code](https://github.com/benxcorp/databricks/blob/main/src/data_analytics/mart/we_mart/wv_server_log_base.py)

### Airflow DAGs

- [analytics_we_mart_wv1_daily](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_we_mart_wv1_daily.py)
- [analytics_log_daily](https://github.com/benxcorp/dp-airflow/blob/main/dags/utils/dynamic_dag/wev/task_list/analytics_log_daily.py)
  
    
---
# DEPENDENCIES

## 👨‍👩‍👧‍👦 Up/Downstream Table List

|Upstream Tables|Downstream Tables|
| :--- | :--- |
|service_log.weverse_server_log|we_mart.stats_wv_d_pop_search_term|
| |we_mart.stats_wv_w_pop_search_term|
| |we_mart.wv_live_play|
| |we_mart.wv_post_view|
| |we_mart.wv_report|
| |we_mart.wv_sess_daily|
| |we_mart.wv_user_ctry_history|
| |we_mart.wv_vod_play|

## 🐤 Downstream Tables Info
  
### Downstream Tables
- **wv_server_log_hourly** : 서버 로그 hourly 단위로 저장
    - we_mart.view_wv_server_log_base 테이블에서 date_id와 hour를 파티션 키로 하여 사용
    - 특정 날짜와 시간대의 사용자 정보, 로그 정보, 국가 정보 등을 조회
    - ```python
      # 특정 날짜, 시간대의 사용자 정보 조회
      q = spark.sql(f"""
        select *
        from we_mart.wv_server_log_hourly
        where date_id = '2023-08-09' and hour = '12'
      """)
      df = q.show()
      ```
- **wv_sess_daily** : 세션, 유저, 커뮤니티별 방문 기록 저장
    - we_mart.wv_server_log_base 테이블에서 date_id를 파티션 키로 하여 사용
    - 특정 날짜의 세션 정보, 유저 정보, 커뮤니티 정보 등을 조회
    - ```python
      # 특정 날짜의 특정 유저의 세션 정보 조회
      q = spark.sql(f"""
        select *
        from we_mart.wv_sess_daily
        where part_date = '2023-08-10' and wv_user_id = 123456
      """)
      df = q.show()
      ```
- **stats_wv_w_pop_search_term** : 주간 주요 검색 키워드 통계 저장
    - we_mart.wv_server_log_base 테이블에서 date_id를 파티션 키로 하여 사용
    - 특정 기간 동안의 검색 키워드, 검색 횟수, 검색 세션 수, 검색 유저 수, 순위, 순위 변동 등을 조회
    - ```python
      # 특정 기간 동안 특정 아티스트의 주요 검색 키워드 조회
      q = spark.sql(f"""
        select *
        from we_mart.stats_wv_w_pop_search_term
        where part_date between '2023-08-06' and '2023-08-12' and we_art_id = 123456
      """)
      df = q.show()
      ```
- **wv_vod_play** : VOD, LIVE to VOD, Youtube 재생 서버 로그 저장
    - we_mart.wv_server_log_base 테이블에서 date_id를 파티션 키로 하여 사용
    - 특정 날짜의 VOD, LIVE to VOD, Youtube 재생 정보, 유저 정보, 아티스트 정보 등을 조회
    - ```python
      # 특정 날짜의 특정 아티스트의 VOD 재생 정보 조회
      q = spark.sql(f"""
        select *
        from we_mart.wv_vod_play
        where part_date = '2023-08-10' and we_art_id = 123456
      """)
      df = q.show()
      ```

### Downstream View Tables
- **view_wv_server_log_base** : 스트리밍 서버 로그 뷰 테이블
    - wev_prod.streaming_log.weverse_server_log 테이블을 소스로 하여 생성
    - 최근 1일치 및 다음 1일치의 로그 데이터를 뷰 테이블 형태로 제공
    - we_mart.wv_server_log_hourly, we_mart.wv_sess_daily, we_mart.stats_wv_w_pop_search_term 등의 테이블에서 사용
    - ```python
      # 최근 1일치 및 다음 1일치의 로그 데이터 조회
      q = spark.sql(f"""
        select *
        from we_mart.view_wv_server_log_base
      """)
      df = q.show()
      ```  
---