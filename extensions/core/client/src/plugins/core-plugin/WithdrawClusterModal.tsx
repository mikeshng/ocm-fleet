import {
  Alert,
  Button,
  Content,
  FormGroup,
  FormHelperText,
  HelperText,
  HelperTextItem,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  TextArea,
} from "@patternfly/react-core";
import { useEffect, useState } from "react";

import {
  withdrawCluster,
  type WithdrawConflict,
  withdrawConflict,
} from "./enrollmentApi";

/** The server measures the withdrawn reason in bytes and refuses more. */
const MAX_WITHDRAW_REASON_BYTES = 500;

interface WithdrawClusterModalProps {
  isOpen: boolean;
  clusterId: string;
  service: string;
  onClose: () => void;
  /** Called after the cluster's record says withdrawn. */
  onWithdrawn: () => void;
}

export default function WithdrawClusterModal({
  isOpen,
  clusterId,
  service,
  onClose,
  onWithdrawn,
}: WithdrawClusterModalProps) {
  const [reason, setReason] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [conflict, setConflict] = useState<WithdrawConflict | null>(null);
  const [submitting, setSubmitting] = useState(false);

  // The reason is sent trimmed, and the server counts bytes; measure the
  // same string Go would see.
  const reasonBytes = new TextEncoder().encode(reason.trim()).length;
  const reasonTooLong = reasonBytes > MAX_WITHDRAW_REASON_BYTES;

  useEffect(() => {
    if (!isOpen) return;
    setReason("");
    setError(null);
    setConflict(null);
    setSubmitting(false);
  }, [isOpen, clusterId]);

  const submit = async (leaveDeployments: boolean) => {
    if (reasonTooLong) return;
    setSubmitting(true);
    setError(null);
    try {
      await withdrawCluster(service, clusterId, { reason, leaveDeployments });
      onWithdrawn();
    } catch (e) {
      const found = withdrawConflict(e);
      if (found) {
        setConflict(found);
      } else {
        setError(
          e instanceof Error ? e.message : "The cluster was not withdrawn",
        );
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      isOpen={isOpen}
      onClose={submitting ? undefined : onClose}
      variant="small"
    >
      <ModalHeader
        title="Withdraw cluster"
        description={`Withdraw "${clusterId}"? The record stays, marked withdrawn, and its credential and target are removed. Nothing on the cluster itself is touched.`}
      />
      <ModalBody>
        {conflict && (
          <Alert
            variant="warning"
            isInline
            title="Deployments still resolve to this cluster"
          >
            <Content component="p">
              These deployments still resolve to target{" "}
              <code>{conflict.target}</code>. Withdrawing and leaving them keeps
              what they delivered on the cluster; nothing here removes or
              updates it afterwards.
            </Content>
            <ul>
              {conflict.deployments.map((deployment) => (
                <li key={deployment}>
                  <code>{deployment}</code>
                </li>
              ))}
            </ul>
          </Alert>
        )}
        {error && (
          <Alert
            variant="danger"
            isInline
            title="The cluster was not withdrawn"
          >
            {error}
          </Alert>
        )}
        <FormGroup
          label="Reason (optional)"
          fieldId="withdraw-cluster-reason"
          className="pf-v6-u-mt-md"
        >
          <TextArea
            id="withdraw-cluster-reason"
            value={reason}
            onChange={(_e, value) => setReason(value)}
            resizeOrientation="vertical"
            aria-label="Reason for withdrawing the cluster"
            validated={reasonTooLong ? "error" : "default"}
            aria-invalid={reasonTooLong}
          />
          <FormHelperText>
            <HelperText>
              <HelperTextItem variant={reasonTooLong ? "error" : "default"}>
                {reasonTooLong
                  ? `The reason is ${reasonBytes} bytes; at most ${MAX_WITHDRAW_REASON_BYTES} are allowed.`
                  : `At most ${MAX_WITHDRAW_REASON_BYTES} bytes.`}
              </HelperTextItem>
            </HelperText>
          </FormHelperText>
        </FormGroup>
        <Content component="small" className="pf-v6-u-mt-sm">
          Kept in the cluster&apos;s record.
        </Content>
      </ModalBody>
      <ModalFooter>
        {conflict ? (
          <Button
            variant="danger"
            onClick={() => submit(true)}
            isLoading={submitting}
            isDisabled={submitting || reasonTooLong}
          >
            Withdraw and leave them
          </Button>
        ) : (
          <Button
            variant="danger"
            onClick={() => submit(false)}
            isLoading={submitting}
            isDisabled={submitting || reasonTooLong}
          >
            Withdraw
          </Button>
        )}
        <Button variant="link" onClick={onClose} isDisabled={submitting}>
          Cancel
        </Button>
      </ModalFooter>
    </Modal>
  );
}
