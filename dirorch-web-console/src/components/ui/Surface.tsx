import clsx from 'clsx'
import type { ElementType, HTMLAttributes, ReactNode } from 'react'

type SurfaceElement = Extract<ElementType, 'article' | 'aside' | 'div' | 'section'>
type SurfacePadding = 'none' | 'sm' | 'md' | 'lg'
type SurfaceRadius = 'sm' | 'md' | 'lg' | 'xl'

interface SurfaceProps extends HTMLAttributes<HTMLElement> {
  as?: SurfaceElement
  children: ReactNode
  padding?: SurfacePadding
  radius?: SurfaceRadius
}

export function Surface({
  as: Component = 'div',
  children,
  className,
  padding = 'md',
  radius = 'lg',
  ...props
}: SurfaceProps) {
  return (
    <Component
      className={clsx(
        'surface',
        `surface--padding-${padding}`,
        `surface--radius-${radius}`,
        className,
      )}
      {...props}
    >
      {children}
    </Component>
  )
}
