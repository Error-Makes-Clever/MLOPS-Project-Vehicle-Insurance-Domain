from src.entity.config_entity import ModelEvaluationConfig
from src.entity.artifact_entity import DataTransformationArtifact, ModelTrainerArtifact, ModelEvaluationArtifact
from sklearn.metrics import f1_score
from src.exception import MyException
from src.logger import logging
from src.utils.main_utils import write_yaml_file, read_yaml_file
import sys
import pandas as pd
from typing import Optional
from src.entity.s3_estimator import Proj1Estimator
from dataclasses import dataclass

@dataclass
class EvaluateModelResponse:

    trained_model_f1_score : float
    best_model_f1_score : float
    is_model_accepted : bool
    difference : float


class ModelEvaluation:

    def __init__(self, model_eval_config : ModelEvaluationConfig, model_trainer_artifact : ModelTrainerArtifact):

        try:
            self.model_eval_config = model_eval_config
            self.model_trainer_artifact = model_trainer_artifact

        except Exception as e:
            raise MyException(e, sys) from e

    def get_best_model(self) -> Optional[Proj1Estimator]:

        """
        Method Name :   get_best_model
        Description :   This function is used to get model from production stage.
        
        Output      :   Returns model object if available in s3 storage
        On Failure  :   Write an exception log and then raise an exception
        """

        try:
            bucket_name = self.model_eval_config.bucket_name
            model_path = self.model_eval_config.s3_model_path
            model_metric_path = self.model_eval_config.s3_model_metric_path
            proj1_estimator = Proj1Estimator(bucket_name = bucket_name, model_path = model_path, model_metric_path = model_metric_path)

            if proj1_estimator.is_model_present(model_path = model_path) and proj1_estimator.is_model_metric_present(model_metric_path = model_metric_path):
                return proj1_estimator
            
            return None
        
        except Exception as e:
            raise  MyException(e,sys)
    

    def evaluate_model(self) -> EvaluateModelResponse:

        """
        Method Name :   evaluate_model
        Description :   This function is used to evaluate trained model 
                        with production model and choose best model 
        
        Output      :   Returns bool value based on validation results
        On Failure  :   Write an exception log and then raise an exception
        """

        try:

            logging.info("Initialized Model Evaluation Component.")

            logging.info("Loading trained model metric artifact.")
            trained_model_metric_path = self.model_trainer_artifact.train_metric_artifact_path

            trained_model_metric = read_yaml_file(file_path = trained_model_metric_path)
            trained_model_f1_score = trained_model_metric["f1_score"]

            logging.info("Loaded trained model metric artifact.")
            logging.info(f"F1_Score for this model: {trained_model_f1_score}")

            best_model_f1_score = None
            best_model = self.get_best_model()

            if best_model is not None:

                logging.info(f"Retriving metric of best model from s3 bucket.")
                best_model_f1_score = best_model.load_metrics()["f1_score"]
                
                logging.info(f"F1_Score-Production Model: {best_model_f1_score}, F1_Score-New Trained Model : {trained_model_f1_score}")
            
            tmp_best_model_score = 0 if best_model_f1_score is None else best_model_f1_score

            result = EvaluateModelResponse(trained_model_f1_score = trained_model_f1_score,
                                           best_model_f1_score = best_model_f1_score,
                                           is_model_accepted = trained_model_f1_score > tmp_best_model_score,
                                           difference = trained_model_f1_score - tmp_best_model_score)
            
            comparison_report = {
                "trained_model_f1_score" : result.trained_model_f1_score,
                "best_model_f1_score" : result.best_model_f1_score,
                "is_model_accepted" : result.is_model_accepted,
                "difference" : result.difference
            }

            logging.info(f"Writing model evaluation report to {self.model_eval_config.model_evaluation_report_file_path}")
            write_yaml_file(file_path = self.model_eval_config.model_evaluation_report_file_path, content = comparison_report)
             
            logging.info(f"Result: {comparison_report}")
            logging.info(
                f"Evaluation Summary | New: {trained_model_f1_score}, "
                f"Old: {best_model_f1_score}, Accepted: {trained_model_f1_score > (best_model_f1_score or 0)}"
            )

            logging.info("Model Evaluation Completed.") 
            return result

        except Exception as e:
            raise MyException(e, sys)

    def initiate_model_evaluation(self) -> ModelEvaluationArtifact:

        """
        Method Name :   initiate_model_evaluation
        Description :   This function is used to initiate all steps of the model evaluation
        
        Output      :   Returns model evaluation artifact
        On Failure  :   Write an exception log and then raise an exception
        """  

        try:

            print("------------------------------------------------------------------------------------------------")
            logging.info("Initialized Model Evaluation Component.")

            evaluate_model_response = self.evaluate_model()
            s3_model_path = self.model_eval_config.s3_model_path

            model_evaluation_artifact = ModelEvaluationArtifact(
                is_model_accepted = evaluate_model_response.is_model_accepted,
                s3_model_path = s3_model_path,
                s3_model_metric_path = self.model_eval_config.s3_model_metric_path,
                trained_model_path = self.model_trainer_artifact.trained_model_file_path,
                train_metric_artifact_path = self.model_trainer_artifact.train_metric_artifact_path,
                changed_accuracy = evaluate_model_response.difference,
                evaluation_comparision_report_path = self.model_eval_config.model_evaluation_report_file_path)

            logging.info(f"Model evaluation artifact: {model_evaluation_artifact}")
            return model_evaluation_artifact
        
        except Exception as e:
            raise MyException(e, sys) from e