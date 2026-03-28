from transforms.api import transform, Input, Output, LightweightInput, LightweightOutput
from palantir_models.transforms import ModelInput
from palantir_models import ModelAdapter


@transform(
    testing_data_input=Input("ri.foundry.main.dataset.084a62cb-86a9-4736-9d76-aece0df5a98c"),
    model_input=ModelInput("/inmind.ai/Temporary Training Artifacts/Learning - Peter El Bikai/Final Project/code repos/models/ipo_pre_ipo_model"),
    predictions_output=Output("ri.foundry.main.dataset.87b21df2-4ac4-4680-ae0d-aaff0714545a"),
)
def compute(
    testing_data_input: LightweightInput,
    model_input: ModelAdapter,
    predictions_output: LightweightOutput,
):
    inference_outputs = model_input.transform(testing_data_input)
    predictions_output.write_pandas(inference_outputs.df_out)
