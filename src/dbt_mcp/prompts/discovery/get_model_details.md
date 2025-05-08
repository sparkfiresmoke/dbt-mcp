<instructions>
Retrieves information about a specific dbt model. Specifically, it returns the compiled sql, description, column names, column descriptions, and column types.

You can provide either a model_name or a uniqueId, if known, to identify the model. Using uniqueId is more precise and guarantees a unique match, which is especially useful when models might have the same name or alias in different projects.
</instructions>

<parameters>
model_name: The name of the dbt model to retrieve details for.
uniqueId: (Optional) The unique identifier of the model. If provided, this will be used instead of model_name for a more precise lookup. You can get the uniqueId from the get_all_models() tool.
</parameters>

<examples>
1. Getting details for a model by name:
   get_model_details(model_name="customer_orders")

2. Getting details for a model by uniqueId (more precise):
   get_model_details(model_name="customer_orders", uniqueId="model.my_project.customer_orders")
   
3. Getting details using only uniqueId:
   get_model_details(model_name="", uniqueId="model.my_project.customer_orders")
</examples>