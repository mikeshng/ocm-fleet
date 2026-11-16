import {
  Alert,
  Form,
  FormGroup,
  FormHelperText,
  HelperText,
  HelperTextItem,
  TextInput,
} from "@patternfly/react-core";

import { kindClusterNameError } from "./api";
import type { ClusterFormData } from "./CreateClusterWizard";

interface ClusterDetailsStepProps {
  formData: ClusterFormData;
  onChange: <K extends keyof ClusterFormData>(
    field: K,
    value: ClusterFormData[K],
  ) => void;
}

export default function ClusterDetailsStep({
  formData,
  onChange,
}: ClusterDetailsStepProps) {
  const nameError = kindClusterNameError(formData.name.trim());
  return (
    <Form>
      <Alert variant="info" isInline isPlain title="">
        This creates a kind cluster on the local agent with optimized control
        plane settings.
      </Alert>

      <FormGroup label="Cluster name" isRequired fieldId="cluster-name">
        <TextInput
          id="cluster-name"
          isRequired
          value={formData.name}
          onChange={(_e, val) => onChange("name", val)}
          placeholder="my-cluster"
          validated={formData.name.trim() && nameError ? "error" : "default"}
        />
        {nameError && (
          <FormHelperText>
            <HelperText>
              <HelperTextItem
                variant={formData.name.trim() ? "error" : "indeterminate"}
              >
                {nameError}
              </HelperTextItem>
            </HelperText>
          </FormHelperText>
        )}
      </FormGroup>

      <FormGroup label="Node image" fieldId="node-image">
        <TextInput
          id="node-image"
          value={formData.nodeImage}
          onChange={(_e, val) => onChange("nodeImage", val)}
          placeholder="kindest/node:v1.31.0"
        />
      </FormGroup>
    </Form>
  );
}
