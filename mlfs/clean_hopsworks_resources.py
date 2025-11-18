import logging

import hopsworks

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def delete_deployment(deployment_name):
    try:
        deployment = ms.get_deployment(name=deployment_name)
        logger.info(f"Deleting deployment: {deployment.name}")
        deployment.stop()
        try:
            deployment.delete()
        except Exception:
            logger.error(f"Problem deleting deployment: {deployment_name}.")
    except Exception:
        logger.warning("No deployments to delete.")


def delete_model(model_name):
    try:
        models = mr.get_models(name=model_name)
        for model in models:
            logger.info(f"Deleting model: {model.name} (version: {model.version})")
            try:
                model.delete()
            except Exception:
                logger.error(f"Failed to delete model {model_name}.")
    except Exception:
        logging.warning("No  models to delete.")


def delete_feature_view(feature_view):
    # Get all feature views
    try:
        feature_views = fs.get_feature_views(name=feature_view)
    except:
        logger.warning(f"Couldn't find feature view: {feature_view}. Skipping...")
        feature_views = []

    # Delete each feature view
    for fv in feature_views:
        logger.info(f"Deleting feature view: {fv.name} (version: {fv.version})")
        try:
            fv.delete()
        except Exception:
            logger.warning(f"Failed to delete feature view {fv.name}.")


def delete_feature_group(feature_group):
    # Get all feature groups
    try:
        feature_groups = fs.get_feature_groups(name=feature_group)
    except:
        logger.warning(f"Couldn't find feature group: {feature_group}. Skipping...")
        feature_groups = []

    # Delete each feature group
    for fg in feature_groups:
        logger.info(f"Deleting feature group: {fg.name} (version: {fg.version})")
        try:
            fg.delete()
        except:
            logger.warning(f"Failed to delete feature group {fg.name}.")


if __name__ == "__main__":
    project = hopsworks.login()

    # Get feature store, deployment registry, model registry
    fs = project.get_feature_store()
    ms = project.get_model_serving()
    mr = project.get_model_registry()

    delete_model("air_quality_xgboost_model")
    delete_model("air_quality_lagged_xgboost_model")
    delete_feature_view("air_quality_fv")
    delete_feature_view("air_quality_lagged_fv")
    for feature_group in [
        "air_quality",
        "weather",
        "air_quality_lagged",
        "air_quality_predictions",
        "air_quality_lagged_predictions",
    ]:
        delete_feature_group(feature_group)
