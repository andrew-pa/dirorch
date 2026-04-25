import * as Dialog from '@radix-ui/react-dialog'
import { X } from 'lucide-react'

import { BackendEndpointControl } from './BackendEndpointControl'

interface SettingsModalProps {
  defaultBackendEndpoint: string
  backendEndpoint: string
  open: boolean
  onBackendEndpointChange: (endpoint: string) => void
  onBackendEndpointReset: () => void
  onOpenChange: (open: boolean) => void
}

export function SettingsModal({
  defaultBackendEndpoint,
  backendEndpoint,
  open,
  onBackendEndpointChange,
  onBackendEndpointReset,
  onOpenChange,
}: SettingsModalProps) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="dialog-content settings-dialog">
          <header className="dialog-header settings-dialog__header">
            <Dialog.Title className="dialog-title">Settings</Dialog.Title>
            <Dialog.Close asChild>
              <button className="icon-button" type="button" aria-label="Close settings">
                <X size={17} />
              </button>
            </Dialog.Close>
          </header>
          <div className="dialog-body settings-dialog__body">
            <BackendEndpointControl
              defaultEndpoint={defaultBackendEndpoint}
              endpoint={backendEndpoint}
              key={backendEndpoint}
              onChange={onBackendEndpointChange}
              onReset={onBackendEndpointReset}
            />
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
