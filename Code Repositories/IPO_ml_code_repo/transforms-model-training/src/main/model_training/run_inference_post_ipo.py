from transforms.api import transform, Input, Output, LightweightInput, LightweightOutput
from palantir_models.transforms import ModelInput
from palantir_models import ModelAdapter


@transform(
    testing_data_input=Input("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/datasets/machinelearning/ml_testing"),
    model_input=ModelInput("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/models/ipo_post_ipo_model"),
    predictions_output=Output("ri.foundry.main.dataset.8c373bd8-3b21-47bf-8ba8-edfd80272efd"),
)
def compute(
    testing_data_input: LightweightInput,
    model_input: ModelAdapter,
    predictions_output: LightweightOutput,
):
    inference_outputs = model_input.transform(testing_data_input)
    predictions_output.write_pandas(inference_outputs.df_out)
