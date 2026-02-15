"""
Email Reporter - 일별 포트홀 탐지 요약 이메일 전송

mv_daily_summary 뷰에서 데이터를 조회하여
Gmail SMTP로 일별 요약 이메일을 전송.
"""

import os
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from sqlalchemy import text

from loaders.base_loader import BaseLoader


class EmailReporter(BaseLoader):
    """이메일 보고서 생성 및 전송"""

    def __init__(self, config_path="config.yaml"):
        """초기화"""
        super().__init__(config_path)
        self.gmail_address = os.environ.get("GMAIL_ADDRESS")
        self.gmail_password = os.environ.get("GMAIL_PASSWORD")

        if not self.gmail_address or not self.gmail_password:
            self.logger.warning(
                "Gmail credentials not set. "
                "Set GMAIL_ADDRESS and GMAIL_PASSWORD environment variables."
            )

    def get_daily_summary(self, date=None):
        """
        지정 날짜의 포트홀 탐지 요약 조회

        Args:
            date: 조회 날짜 (YYYY-MM-DD, 기본값: 어제)

        Returns:
            dict: 일일 요약 통계
        """
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        query = f"""
            SELECT
                date,
                affected_segments,
                total_impacts,
                critical_segments
            FROM mv_daily_summary
            WHERE date = '{date}'
        """

        try:
            result = self.fetch_query(query)
            if result:
                row = result[0]
                return {
                    "date": str(row[0]),
                    "affected_segments": row[1],
                    "total_impacts": row[2],
                    "critical_segments": row[3],
                }
            else:
                self.logger.warning(f"No data found for {date}")
                return None
        except Exception as e:
            self.logger.error(f"Failed to fetch daily summary: {e}")
            raise

    def get_top_segments(self, date=None, limit=5):
        """
        지정 날짜의 상위 보수 우선순위 세그먼트

        Args:
            date: 조회 날짜 (YYYY-MM-DD, 기본값: 어제)
            limit: 상위 개수 (기본값: 5)

        Returns:
            list: 상위 세그먼트 정보
        """
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        query = f"""
            SELECT
                s_id,
                total_impacts,
                avg_daily_impacts,
                detected_days,
                priority_score
            FROM mv_repair_priority
            ORDER BY priority_score DESC
            LIMIT {limit}
        """

        try:
            result = self.fetch_query(query)
            segments = []
            for row in result:
                segments.append({
                    "s_id": row[0],
                    "total_impacts": row[1],
                    "avg_daily_impacts": float(row[2]),
                    "detected_days": row[3],
                    "priority_score": float(row[4]),
                })
            return segments
        except Exception as e:
            self.logger.error(f"Failed to fetch top segments: {e}")
            raise

    def generate_email_body(self, date=None):
        """
        이메일 본문 생성

        Args:
            date: 보고 날짜 (YYYY-MM-DD, 기본값: 어제)

        Returns:
            str: HTML 형식의 이메일 본문
        """
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        summary = self.get_daily_summary(date)
        top_segments = self.get_top_segments(date, limit=5)

        if not summary:
            return f"<p>No data available for {date}</p>"

        # HTML 이메일 본문
        html = f"""
        <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
                <h2>🚗 포트홀 탐지 시스템 - 일일 요약 보고서</h2>
                <hr style="border: none; border-top: 2px solid #007bff;">

                <h3>📊 {date} 통계</h3>
                <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
                    <tr style="background-color: #f8f9fa;">
                        <td style="padding: 10px; border: 1px solid #ddd;">
                            <strong>탐지된 세그먼트</strong>
                        </td>
                        <td style="padding: 10px; border: 1px solid #ddd; font-size: 18px; color: #007bff;">
                            <strong>{summary['affected_segments']}</strong>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;">
                            <strong>총 탐지 횟수</strong>
                        </td>
                        <td style="padding: 10px; border: 1px solid #ddd; font-size: 18px; color: #28a745;">
                            <strong>{summary['total_impacts']}</strong>
                        </td>
                    </tr>
                    <tr style="background-color: #f8f9fa;">
                        <td style="padding: 10px; border: 1px solid #ddd;">
                            <strong>위험(Critical) 세그먼트</strong>
                        </td>
                        <td style="padding: 10px; border: 1px solid #ddd; font-size: 18px; color: #dc3545;">
                            <strong>{summary['critical_segments']}</strong>
                        </td>
                    </tr>
                </table>

                <hr style="border: none; border-top: 1px solid #ddd; margin: 20px 0;">

                <h3>🔴 상위 보수 우선순위 세그먼트 (Top 5)</h3>
                <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
                    <thead style="background-color: #007bff; color: white;">
                        <tr>
                            <th style="padding: 10px; text-align: left;">세그먼트 ID</th>
                            <th style="padding: 10px; text-align: right;">총 탐지</th>
                            <th style="padding: 10px; text-align: right;">일평균</th>
                            <th style="padding: 10px; text-align: right;">탐지 일수</th>
                            <th style="padding: 10px; text-align: right;">우선순위 점수</th>
                        </tr>
                    </thead>
                    <tbody>
        """

        for i, seg in enumerate(top_segments):
            row_color = "#ffffff" if i % 2 == 0 else "#f8f9fa"
            html += f"""
                        <tr style="background-color: {row_color};">
                            <td style="padding: 10px; border: 1px solid #ddd;">
                                <strong>{seg['s_id']}</strong>
                            </td>
                            <td style="padding: 10px; border: 1px solid #ddd; text-align: right;">
                                {seg['total_impacts']}
                            </td>
                            <td style="padding: 10px; border: 1px solid #ddd; text-align: right;">
                                {seg['avg_daily_impacts']:.2f}
                            </td>
                            <td style="padding: 10px; border: 1px solid #ddd; text-align: right;">
                                {seg['detected_days']}
                            </td>
                            <td style="padding: 10px; border: 1px solid #ddd; text-align: right;">
                                {seg['priority_score']:.2f}
                            </td>
                        </tr>
            """

        html += """
                    </tbody>
                </table>

                <hr style="border: none; border-top: 1px solid #ddd; margin: 30px 0;">
                <footer style="font-size: 12px; color: #999; text-align: center;">
                    <p>이 보고서는 자동으로 생성되었습니다. | 포트홀 탐지 시스템</p>
                </footer>
            </body>
        </html>
        """

        return html

    def send_email(self, recipient_email, date=None):
        """
        Gmail SMTP로 이메일 전송

        Args:
            recipient_email: 받는사람 이메일 주소
            date: 보고 날짜 (YYYY-MM-DD, 기본값: 어제)

        Returns:
            bool: 전송 성공 여부
        """
        if not self.gmail_address or not self.gmail_password:
            self.logger.error(
                "Gmail credentials not configured. "
                "Set GMAIL_ADDRESS and GMAIL_PASSWORD environment variables."
            )
            return False

        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            # 이메일 제목 및 본문 생성
            subject = f"[포트홀 탐지] 일일 요약 보고서 - {date}"
            html_body = self.generate_email_body(date)

            # 이메일 객체 생성
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.gmail_address
            msg["To"] = recipient_email

            # HTML 본문 추가
            part = MIMEText(html_body, "html")
            msg.attach(part)

            # Gmail SMTP로 전송
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(self.gmail_address, self.gmail_password)
                server.sendmail(self.gmail_address, recipient_email, msg.as_string())

            self.logger.info(f"Email sent to {recipient_email} for {date}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to send email: {e}")
            return False


def send_daily_report(recipient_email, date=None, config_path="config.yaml"):
    """
    일일 요약 보고서 이메일 전송 (Airflow Task용 메인 함수)

    Args:
        recipient_email: 받는사람 이메일
        date: 보고 날짜 (YYYY-MM-DD, 기본값: 어제)
        config_path: 설정 파일 경로
    """
    reporter = EmailReporter(config_path)
    try:
        reporter.send_email(recipient_email, date)
    finally:
        reporter.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Send daily pothole summary email")
    parser.add_argument("--email", required=True, help="Recipient email address")
    parser.add_argument("--date", help="Report date (YYYY-MM-DD)")
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    parser.add_argument("--test", action="store_true", help="Test mode: show email content without sending")

    args = parser.parse_args()

    reporter = EmailReporter(args.config)

    if args.test:
        # 테스트 모드: 이메일 내용만 출력
        print("\n=== 이메일 테스트 모드 ===\n")
        html_body = reporter.generate_email_body(args.date)
        print(html_body)
        print("\n=== 테스트 완료 ===\n")
    else:
        # 실제 이메일 전송
        reporter.send_email(args.email, args.date)

    reporter.close()