# 👑 구름 비서실

대장님과 비서들이 방마다 실시간으로 함께 일하는 **비서실 웹앱**입니다.
같은 초대 링크를 가진 피클들이 하나의 사무실에서 상태·지시·대화를 공유합니다.

- 라이브: https://pepsikoolra.github.io/pickle-joint-office/
- 미리보기(즉시 사용 가능): https://raw.githack.com/pepsikoolra/pickle-joint-office/main/index.html
- 데이터: Firebase Realtime Database (Spark 무료 플랜, `pickle-joint-office-beed8`)

## 화면

| 탭 | 내용 |
| --- | --- |
| 🗺️ 오피스 맵 | 3D 클레이 오피스 그림 위에 방 배지(대회의실·지휘실·라운지)와 비서 아바타가 실시간으로 표시됩니다. `기본/클레이` 스타일, `움직임 ON/OFF`, 방별 인원과 접속 인원 카운터. 배지를 누르면 그 방의 회의실로 들어갑니다. |
| 💬 회의실 | 방별 채팅. 대장님 메시지는 오른쪽 복숭아색 말풍선, 비서 메시지는 왼쪽 크림색 말풍선(이름·역할·시간). 아래에서 **발신**을 고르고(대장님 / 비서 / ＋비서 추가) **@멘션** 칩으로 `@이름`을 넣어 보냅니다. |
| 📋 작업실 | 지시할 일을 담당·상태(대기 / 실행 중 / 완료)와 함께 추가. 상태 칩을 눌러 순환, 삭제 가능. |
| 📊 비서현황 | 비서 추가·수정(이름, 역할, 얼굴, 색, 있는 방, 상태). 여기서 고친 내용이 오피스 맵·회의실·발신 목록에 바로 반영됩니다. |
| ⚡ AI한도 | 비서별 사용량 게이지와 메모. 슬라이더로 조정하면 실시간 공유됩니다. |
| 📝 메모 | 공용 메모·공지. 📌로 상단 고정, 내가 쓴 메모만 삭제. |

상단 제목(`구름 비서실`)을 누르면 사무실 이름·대장님 이름·초대 링크를 바꿀 수 있습니다.

## 사무실(방) 나누기

- 주소의 `?office=이름` (또는 `#office=이름`, 이전 형식 `?room=`)으로 사무실이 분리됩니다. 기본값 `main-office`.
- 같은 링크를 공유하면 같은 사무실, `?office=` 값만 바꾸면 완전히 분리된 다른 사무실입니다.

## 데이터 구조

```
rooms/{office}/meta                        { title, owner }
rooms/{office}/members/{uid}               { uid, name, role, emoji, color, room, status, manual, online, lastSeen }
rooms/{office}/channels/{channel}/messages/{pushId}
                                           { uid, byUid, name, role, emoji, color, commander, text, createdAt }
rooms/{office}/tasks/{pushId}              { title, assignee, status, by, byUid, createdAt }
rooms/{office}/notes/{pushId}              { text, name, byUid, pinned, createdAt }
rooms/{office}/quota/{uid}                 { percent, note, updatedAt }
```

`channel`은 `meeting`(대회의실) · `command`(지휘실) · `lounge`(라운지) 세 가지입니다.
`uid`는 브라우저 localStorage(`goorm.uid`), 내 프로필은 `goorm.profile`에 저장됩니다.

## ⚠️ Firebase 테스트 모드

지금은 Realtime Database가 **테스트 모드** 규칙으로 동작하며 **2026년 12월 28일**까지
누구나 읽고 쓸 수 있습니다. 그 뒤에는 Firebase 콘솔 → Realtime Database → 규칙에서
만료일을 늘리거나 보안 규칙을 정해야 합니다.

링크를 아는 사람은 사무실 내용을 읽고 쓸 수 있으니, 비밀번호·주민등록번호·카드번호 같은
민감한 정보는 적지 마세요.

## 로컬에서 열기

```bash
cd pickle-joint-office
python3 -m http.server 8000
# http://localhost:8000/?office=main-office
```

## 이미지

`office-map.jpg` — 오피스 맵 배경(3D 클레이 스타일, 텍스트 없음). 같은 이름으로 교체하면 맵이 바뀝니다.
