import clsx from 'clsx'
import type { ReactNode } from 'react'

import { Surface } from './Surface'

interface InfoCardProps {
  className?: string
  children?: ReactNode
  label: ReactNode
  meta?: ReactNode
  value?: ReactNode
}

export function InfoCard({ children, className, label, meta, value }: InfoCardProps) {
  return (
    <Surface className={clsx('info-card', className)} padding="sm" radius="sm">
      <div className="eyebrow">{label}</div>
      {value !== undefined && value !== null ? <div className="info-card__value">{value}</div> : null}
      {meta !== undefined && meta !== null ? <div className="info-card__meta">{meta}</div> : null}
      {children}
    </Surface>
  )
}
