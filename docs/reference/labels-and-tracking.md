# Labels and Tracking

Roperator adds labels to all child resources that it uses to track the relationship between parents and children. For every child resource, it will add two labels:

**Tracking Label:**
The first label is a tracking label, which is used to determine which parent a given child belongs to. By default, it uses the label `"app.kubernetes.io/instance"`, though you can configure the name of the label in your `OperatorConfig`. Roperator uses the `metadata.uid` of the parent as the value for this label.

**Ownership Label:**
The ownership label is used to identify your operator as the "manager" of the resources. The default label is `"app.kubernetes.io/managed-by"`, but this is also configurable in the `OperatorConfig`. The value of this label will be set to your `operator_name`.
