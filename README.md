# 🥒 합동 비서실

다른 피클(Pickle)들과 함께 쓰는 **실시간 합동 비서실** 웹앱입니다.
같은 방 링크를 가진 피클들이 한 화면에서 상태·메모·할 일을 실시간으로 공유합니다.

- 라이브: https://pepsikoolra.github.io/pickle-joint-office/
- 데이터: Firebase Realtime Database (Spark 무료 플랜, `pickle-joint-office-beed8`)

## 기능

| 구역 | 내용 |
| --- | --- |
| 우리들 (상태 보드) | 이름·얼굴·색·상태(업무 중 / 대기 중 / 회의 중 / 휴식 중)를 실시간 공유. 접속 중이면 초록 점, 나가면 자동으로 오프라인 표시. |
| 메모 · 공지 | 실시간 메모/채팅. 📌로 공지를 상단 고정, 내가 쓴 메모만 삭제 가능. Enter 전송 / Shift+Enter 줄바꿈. |
| 할 일 | 제목 + 담당자 + 중요도(높음·보통·낮음)로 추가, 체크로 완료/해제, 삭제. 완료되지 않은 일이 위로. |
| 방 코드 | 주소의 `?room=이름`(또는 `#room=이름`)으로 방 분리. 기본값 `main-office`. 방마다 데이터가 완전히 분리됩니다. |

## 방 링크 공유

- 상단 **링크 복사** 버튼 → `https://pepsikoolra.github.io/pickle-joint-office/?room=<방이름>`
- 받은 사람은 그 링크만 열면 같은 방에 들어옵니다.
- 새 방을 만들려면 주소 끝의 `room=` 값만 바꿔도 되고, 안내(ⓘ) 창의 **다른 방으로 이동**을 써도 됩니다.

## 데이터 구조

```
rooms/{room}/members/{uid}   { uid, name, emoji, color, status, online, lastSeen }
rooms/{room}/messages/{pushId} { uid, name, emoji, color, text, pinned, createdAt }
rooms/{room}/todos/{pushId}    { uid, name, title, assignee, priority, done, createdAt }
```

`uid`는 브라우저 localStorage(`pjo.uid`)에 저장되어 "내 메모만 삭제" 같은 판단에 쓰입니다.
이름·얼굴·색·상태는 `pjo.profile`에 저장됩니다.

## ⚠️ Firebase 테스트 모드

지금은 Realtime Database가 **테스트 모드** 규칙으로 동작하며, **2026년 12월 28일**까지
누구나 읽고 쓸 수 있습니다. 그 이후 연결이 끊기므로, 계속 쓰려면 Firebase 콘솔에서
규칙 만료일을 늘리거나 보안 규칙을 정해야 합니다.

테스트 모드 동안에는 링크를 아는 사람이면 누구나 이 방의 내용을 읽고 쓸 수 있으니
비밀번호·주민등록번호·카드번호 같은 민감한 정보는 적지 마세요.

## 로컬에서 열기

```bash
cd pickle-joint-office
python3 -m http.server 8000
# http://localhost:8000/?room=main-office
```

## 배포

`main` 브랜치 루트(`/`)를 GitHub Pages로 게시합니다. 별도 빌드 과정이 없습니다.
