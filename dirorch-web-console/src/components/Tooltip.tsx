import * as RadixTooltip from '@radix-ui/react-tooltip'

import type { ReactNode } from 'react'

interface TooltipProps {
  children: ReactNode
  content: ReactNode
}

export function Tooltip({ children, content }: TooltipProps) {
  return (
    <RadixTooltip.Provider delayDuration={120}>
      <RadixTooltip.Root>
        <RadixTooltip.Trigger asChild>{children}</RadixTooltip.Trigger>
        <RadixTooltip.Portal>
          <RadixTooltip.Content className="tooltip-content" sideOffset={8}>
            {content}
            <RadixTooltip.Arrow className="tooltip-arrow" />
          </RadixTooltip.Content>
        </RadixTooltip.Portal>
      </RadixTooltip.Root>
    </RadixTooltip.Provider>
  )
}

