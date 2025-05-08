<instructions>
Retrieves information about a specific dbt model. Specifically, it returns the compiled sql, description, column names, column descriptions, and column types.

If the model has an alias that differs from its name, you can provide the alias parameter to ensure the correct model is found. When using the alias parameter, the tool may return multiple models if more than one model shares the same alias.
</instructions>

<parameters>
model_name: The name of the dbt model to retrieve details for.
alias: (Optional) The alias of the model, if different from its name. Use this when the model is referenced by an alias in queries. If provided, may return multiple models that share the same alias.
</parameters>

<examples>
1. Getting details for a model by name:
   get_model_details(model_name="customer_orders")

2. Getting details for a model with an alias:
   get_model_details(model_name="customer_orders", alias="cust_orders")
</examples>