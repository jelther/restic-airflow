from airflow.models import Variable
from airflow.utils.email import send_email
from loguru import logger


def dag_failure_callback(context) -> None:
    try:
        alert_email: str = Variable.get("ALERT_EMAIL", default_var="email")
        subject = f"Restic DAG Failure: {context['dag'].dag_id}"
        body = f"""
        DAG: {context['dag']}
        Execution Time: {context['execution_date']}
        Log URL: {context['task_instance'].log_url}
        """
        send_email(alert_email, subject, body)
    except Exception as e:
        logger.error(f"Error sending email on failure: {e}")
        raise Exception(f"Error sending email on failure: {e}")
    else:
        logger.info("Email sent successfully on failure")


def dag_success_callback(context) -> None:
    try:
        alert_email: str = Variable.get("ALERT_EMAIL", default_var="email")
        subject = f"Restic DAG Success: {context['dag'].dag_id}"
        body = f"""
        DAG: {context['dag'].dag_id}
        Execution Time: {context['execution_date']}
        """
        send_email(alert_email, subject, body)
    except Exception as e:
        logger.error(f"Error sending email on success: {e}")
        raise Exception(f"Error sending email on failure: {e}")
    else:
        logger.info("Email sent successfully on success")


def task_failure_callback(context) -> None:
    try:
        alert_email: str = Variable.get("ALERT_EMAIL", default_var="email")
        subject = f"Restic Task Failure: {context['task_instance'].task_id}, DAG: {context['dag'].dag_id}"
        body = f"""
        Task: {context['task_instance'].task_id}
        Execution Time: {context['execution_date']}
        Log URL: {context['task_instance'].log_url}
        """
        send_email(alert_email, subject, body)
    except Exception as e:
        logger.error(f"Error sending email on failure: {e}")
        raise Exception(f"Error sending email on failure: {e}")
    else:
        logger.info("Email sent successfully on failure")


def task_success_callback(context) -> None:
    try:
        alert_email: str = Variable.get("ALERT_EMAIL", default_var="email")
        subject = f"Restic Task Success: {context['task_instance'].task_id}, DAG: {context['dag'].dag_id}"
        body = f"""
        Task: {context['task_instance'].task_id}
        Execution Time: {context['execution_date']}
        """
        send_email(alert_email, subject, body)
    except Exception as e:
        logger.error(f"Error sending email on success: {e}")
        raise Exception(f"Error sending email on failure: {e}")
    else:
        logger.info("Email sent successfully on success")
