from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# =========================================================
# Configuration
# =========================================================

PIPELINE_RUN_ID = "{{ dag_run.run_id }}"


# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="master_dag",
    start_date=datetime(2026, 9, 26),
    schedule=None,
    catchup=False,
    tags=["master", "orchestration", "ecommerce-pipeline"],
) as dag:


    # -----------------------------------------------------
    # 2. Staging validation
    # -----------------------------------------------------

    validation = TriggerDagRunOperator(
        task_id="run_validation",
        trigger_dag_id="validation",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
        conf={
            "pipeline_run_id": PIPELINE_RUN_ID,
        },
    )

    # -----------------------------------------------------
    # 3. Transformation
    # -----------------------------------------------------

    transformation = TriggerDagRunOperator(
        task_id="run_transformation",
        trigger_dag_id="transformation",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
        conf={
            "pipeline_run_id": PIPELINE_RUN_ID,
        },
    )

    # -----------------------------------------------------
    # 4. Curated validation
    # -----------------------------------------------------

    curated_validation = TriggerDagRunOperator(
        task_id="run_curated_validation",
        trigger_dag_id="curated_validation",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
        conf={
            "pipeline_run_id": PIPELINE_RUN_ID,
        },
    )

    # -----------------------------------------------------
    # 5. Presentation
    # -----------------------------------------------------

    presentation = TriggerDagRunOperator(
        task_id="run_presentation",
        trigger_dag_id="presentation",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
        conf={
            "pipeline_run_id": PIPELINE_RUN_ID,
        },
    )

    # =====================================================
    # Pipeline dependency
    # =====================================================

    validation >> transformation >> curated_validation >> presentation