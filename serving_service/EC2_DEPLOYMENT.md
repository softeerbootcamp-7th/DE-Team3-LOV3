# EC2 배포 가이드

## 빠른 시작 (User Data)

EC2 인스턴스 생성 시 User Data에 다음을 붙여넣으면 자동 배포됩니다.

```bash
#!/bin/bash
set -e

# 필수 패키지 설치
sudo apt-get update
sudo apt-get install -y docker.io docker-compose git curl python3

# Docker 시작
sudo systemctl start docker
sudo systemctl enable docker

# 코드 클론 및 배포 디렉토리 준비
cd /home/ec2-user
git clone https://github.com/softeerbootcamp-7th/DE-Team3-LOV3.git
cd DE-Team3-LOV3/serving_service

# 데이터 디렉토리 생성
sudo mkdir -p /data/postgres /data/superset
sudo chmod 755 /data/postgres /data/superset

# 환경 변수 파일 생성 (.env)
cat > .env << 'EOF'
POSTGRES_PASSWORD=your-strong-postgres-password
SUPERSET_SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
SUPERSET_ADMIN_PASSWORD=your-strong-admin-password
POSTGRES_HOST=postgres
S3_BUCKET=your-s3-bucket-name
EOF

# 서비스 시작
sudo docker-compose up -d
```

**주의**: 비밀번호와 S3 버킷명을 실제 값으로 변경하세요.

## 수동 배포

EC2에 SSH 접속 후 다음을 실행합니다.

```bash
# 저장소 클론
cd /home/ec2-user
git clone https://github.com/softeerbootcamp-7th/DE-Team3-LOV3.git
cd DE-Team3-LOV3/serving_service

# 디렉토리 준비
sudo mkdir -p /data/postgres /data/superset
sudo systemctl start docker

# .env 파일 생성
cat > .env << EOF
POSTGRES_PASSWORD=your-strong-postgres-password
SUPERSET_SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
SUPERSET_ADMIN_PASSWORD=your-strong-admin-password
POSTGRES_HOST=postgres
S3_BUCKET=pothole-detection-results
EOF

# 시작
sudo docker-compose up -d
```

## 데이터 적재

```bash
# 의존성 설치
pip install -r requirements.txt

# S3에서 데이터 적재
python loader.py \
  --s3-path "s3://bucket/stage2/parquet/dt=2026-02-12/" \
  --date "2026-02-12" \
  --config config.yaml
```

## 기본 명령어

```bash
# 상태 확인
sudo docker-compose ps

# 로그 보기
sudo docker-compose logs -f

# PostgreSQL 접속
sudo docker-compose exec postgres psql -U pothole_user -d road_safety

# Superset 접속
# 브라우저에서: http://<EC2_IP>:8088
# 계정: admin / .env의 SUPERSET_ADMIN_PASSWORD
```

## 문제 해결

**PostgreSQL 연결 실패**
```bash
sudo docker-compose restart postgres
sudo docker-compose logs postgres
```

**디스크 부족**
```bash
df -h /data
sudo docker system prune
```

**Superset 접속 불가**
```bash
sudo docker-compose restart superset
sudo docker-compose logs superset
```

## 보안 체크리스트

- [ ] `.env`의 비밀번호를 강력하게 설정
- [ ] `.env`는 git에 커밋하지 않기 (.gitignore 확인)
- [ ] EC2 보안 그룹: 8088 포트는 필요한 IP만 개방
- [ ] PostgreSQL 5432는 내부 통신만 (expose 설정)

## IAM Role 설정 (S3 접근)

EC2 인스턴스가 S3에 접근하려면 IAM Role을 부여해야 합니다.

### 1. IAM Role 생성

AWS Management Console:
1. **IAM** → **Roles** → **Create role**
2. **Trusted entity type**: AWS service
3. **Service**: EC2
4. **Permissions**: `AmazonS3ReadOnlyAccess` 추가 (또는 커스텀)
5. **Role name**: `pothole-s3-reader` (예시)

### 2. EC2 인스턴스 시작 시 Role 연결

EC2 인스턴스 생성 시:
- **Advanced Details** → **IAM instance profile** → `pothole-s3-reader` 선택

또는 기존 인스턴스에 연결:
```bash
aws ec2 associate-iam-instance-profile \
  --instance-id i-xxxxxxxxxxxx \
  --iam-instance-profile Name=pothole-s3-reader
```

### 3. 확인

EC2 내에서:
```bash
# 자동 로드된 credentials 확인
curl http://169.254.169.254/latest/meta-data/iam/security-credentials/pothole-s3-reader

# S3 접근 테스트
aws s3 ls s3://your-bucket/
```

