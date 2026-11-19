import {
  Button,
  Content,
  Form,
  FormGroup,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  TextInput,
} from "@patternfly/react-core";
import { useEffect, useId, useState } from "react";

import { deleteConfirmationMatches } from "./deleteConfirmation";

export interface DeleteClusterModalProps {
  clusterId: string;
  isOpen: boolean;
  isDeleting?: boolean;
  onClose: () => void;
  onDelete: () => void;
}

/**
 * The confirmation for deleting a provisioned cluster. Delete stays disabled until the
 * cluster's name is typed: on the Clusters page the rows stand close together and this
 * action terminates a real cluster.
 */
export function DeleteClusterModal({
  clusterId,
  isOpen,
  isDeleting = false,
  onClose,
  onDelete,
}: DeleteClusterModalProps) {
  const [typed, setTyped] = useState("");
  const fieldId = useId();

  useEffect(() => {
    if (isOpen) setTyped("");
  }, [isOpen, clusterId]);

  const confirmed = deleteConfirmationMatches(typed, clusterId);

  return (
    <Modal isOpen={isOpen} onClose={onClose} variant="small">
      <ModalHeader
        title="Delete cluster"
        description={`Are you sure you want to delete "${clusterId}"? This will terminate the provisioned cluster.`}
      />
      <ModalBody>
        <Content component="p" className="pf-v6-u-mb-md">
          Type the cluster&apos;s name to confirm: <strong>{clusterId}</strong>.
        </Content>
        <Form
          onSubmit={(event) => {
            event.preventDefault();
            if (confirmed && !isDeleting) onDelete();
          }}
        >
          <FormGroup label="Cluster name" isRequired fieldId={fieldId}>
            <TextInput
              id={fieldId}
              value={typed}
              onChange={(_event, value) => setTyped(value)}
              isDisabled={isDeleting}
              autoComplete="off"
            />
          </FormGroup>
        </Form>
      </ModalBody>
      <ModalFooter>
        <Button
          variant="danger"
          onClick={onDelete}
          isDisabled={!confirmed || isDeleting}
          isLoading={isDeleting}
        >
          Delete
        </Button>
        <Button variant="link" onClick={onClose} isDisabled={isDeleting}>
          Cancel
        </Button>
      </ModalFooter>
    </Modal>
  );
}

export default DeleteClusterModal;
