import clsx from 'clsx'
import type { HTMLAttributes, ReactNode } from 'react'

interface EmptyStateProps extends HTMLAttributes<HTMLDivElement> {
  children: ReactNode
  icon?: ReactNode
}

export function EmptyState({ children, className, icon, ...props }: EmptyStateProps) {
  return (
    <div className={clsx('empty-state', className)} {...props}>
      {icon}
      {children}
    </div>
  )
}
