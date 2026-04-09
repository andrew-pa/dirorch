import clsx from 'clsx'
import type { ReactNode } from 'react'

interface SectionHeaderProps {
  actions?: ReactNode
  className?: string
  contentClassName?: string
  eyebrow?: ReactNode
  subtitle?: ReactNode
  title?: ReactNode
}

export function SectionHeader({
  actions,
  className,
  contentClassName,
  eyebrow,
  subtitle,
  title,
}: SectionHeaderProps) {
  return (
    <div className={clsx('section-header', className)}>
      <div className={clsx('section-header__content', contentClassName)}>
        {eyebrow ? <div className="eyebrow">{eyebrow}</div> : null}
        {title ? <div className="section-header__title">{title}</div> : null}
        {subtitle ? <div className="section-header__subtitle">{subtitle}</div> : null}
      </div>

      {actions ? <div className="section-header__actions">{actions}</div> : null}
    </div>
  )
}
