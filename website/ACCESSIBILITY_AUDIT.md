# Accessibility Audit Report

**Date:** January 2026
**Scope:** sql-splitter documentation website
**Standard:** WCAG 2.1 AA

---

## Executive Summary

| Category | Status | Issues |
|----------|--------|--------|
| Color Contrast | ✅ Pass | 1 minor |
| Keyboard Navigation | ⚠️ Needs Work | 4 issues |
| Screen Reader | ⚠️ Needs Work | 8 issues |
| Semantic HTML | ✅ Good | 2 minor |
| Focus Management | ⚠️ Needs Work | 3 issues |

**Overall:** The site has a solid foundation but needs improvements for full WCAG 2.1 AA compliance.

---

## Color Contrast Analysis

### Dark Theme (`#0a0a0a` background)

| Element | Color | Contrast Ratio | Status |
|---------|-------|----------------|--------|
| Body text | `#e6edf3` | 17:1 | ✅ Excellent |
| Muted text | `#8b949e` | 7:1 | ✅ Pass |
| Code comments | `#6e7681` | 4.3:1 | ⚠️ Borderline |
| Accent/links | `#58a6ff` | 8.4:1 | ✅ Pass |
| Green (`--cmd-validate`) | `#3fb950` | 6.5:1 | ✅ Pass |
| Orange (`--cmd-convert`) | `#f78166` | 7.2:1 | ✅ Pass |
| Purple (`--cmd-merge`) | `#a371f7` | 5.8:1 | ✅ Pass |

### Light Theme (`#ffffff` background)

| Element | Color | Contrast Ratio | Status |
|---------|-------|----------------|--------|
| Body text | `#24292f` | 14:1 | ✅ Excellent |
| Muted text | `#57606a` | 7:1 | ✅ Pass |
| Accent/links | `#0969da` | 5.8:1 | ✅ Pass |
| Green | `#1a7f37` | 5.2:1 | ✅ Pass |
| Orange | `#bc4c00` | 4.8:1 | ✅ Pass |

### Recommendations

1. **Code comments** (`#6e7681` on `#0a0a0a`): Consider lightening to `#848d97` for 5:1+ ratio
2. All other colors pass WCAG AA requirements

---

## Keyboard Navigation Issues

### Critical

| Issue | Location | Priority |
|-------|----------|----------|
| **No skip link** | All pages | High |
| Missing focus indicators on some buttons | Homepage copy buttons | High |
| Command picker lacks keyboard navigation | CommandBuilder | Medium |

### Missing Skip Link

**Files:** `src/layouts/BaseLayout.astro`, Starlight layout

**Problem:** No "Skip to main content" link for keyboard users to bypass navigation.

**Fix:**
```html
<!-- Add after <body> tag -->
<a href="#main-content" class="skip-link">Skip to main content</a>

<!-- Add to main -->
<main id="main-content">

<!-- Add CSS -->
<style>
.skip-link {
  position: absolute;
  top: -40px;
  left: 0;
  background: var(--color-accent);
  color: var(--color-bg);
  padding: 8px 16px;
  z-index: 1000;
  text-decoration: none;
}
.skip-link:focus {
  top: 0;
}
</style>
```

### Focus Indicators

**File:** `src/pages/index.astro`

**Problem:** Copy buttons lack visible focus ring.

**Fix:**
```css
.copy-btn:focus-visible {
  outline: 2px solid var(--cmd-split);
  outline-offset: 2px;
}
```

---

## Screen Reader Issues

### 1. External Links Missing Announcement

**Files:** `src/pages/index.astro`, `src/layouts/BaseLayout.astro`

**Problem:** External links (GitHub) don't indicate they open in new window.

**Current:**
```html
<a href="https://github.com/..." target="_blank">GitHub</a>
```

**Fix:**
```html
<a href="https://github.com/..." target="_blank" rel="noopener noreferrer">
  GitHub
  <span class="sr-only">(opens in new tab)</span>
</a>
```

### 2. Decorative Icons Not Hidden

**File:** `src/pages/index.astro`

**Problem:** Benefit icons (`~`, `<`, `*`) are read by screen readers.

**Current:**
```html
<div class="benefit-icon">~</div>
```

**Fix:**
```html
<div class="benefit-icon" aria-hidden="true">~</div>
```

### 3. Command Picker Missing ARIA

**File:** `src/components/CommandBuilder.astro`

**Problem:** Command picker acts as a radio group but lacks proper ARIA.

**Current:**
```html
<div class="command-picker">
  <button class="selected">split</button>
  <button>merge</button>
</div>
```

**Fix:**
```html
<div class="command-picker" role="radiogroup" aria-label="Select command">
  <button role="radio" aria-checked="true">split</button>
  <button role="radio" aria-checked="false">merge</button>
</div>
```

### 4. Mode Toggle Missing State

**File:** `src/components/CommandBuilder.astro`

**Problem:** Easy/Pro toggle buttons don't announce selected state.

**Fix:**
```html
<button class="mode-btn active" data-mode="easy" aria-pressed="true">Easy</button>
<button class="mode-btn" data-mode="pro" aria-pressed="false">Pro</button>
```

### 5. Copy Button Missing Accessible Name

**File:** `src/components/CommandBuilder.astro`

**Problem:** Copy button icon-only, lacks accessible label.

**Fix:**
```html
<button class="copy-btn" id="copy-command" aria-label="Copy command to clipboard">
```

### 6. Form Label Association

**File:** `src/components/CommandBuilder.astro`

**Problem:** "Command" label not associated with command picker.

**Fix:**
```html
<label id="command-label">Command</label>
<div class="command-picker" role="radiogroup" aria-labelledby="command-label">
```

### 7. Footer Separators

**File:** `src/layouts/BaseLayout.astro`

**Problem:** Pipe separators (`|`) are read aloud.

**Fix:**
```html
<span class="sep" aria-hidden="true">|</span>
```

### 8. Output Tip Platform Specificity

**File:** `src/components/CommandBuilder.astro`

**Problem:** "Press Ctrl+C" assumes Windows; macOS users need Cmd+C.

**Fix:**
```html
<div class="output-tip">
  Press <kbd><span class="mac-key">⌘</span><span class="win-key">Ctrl</span></kbd>+<kbd>C</kbd> to copy
</div>
```
Or simply: "Click the button above to copy"

---

## Semantic HTML Issues

### 1. Nav Landmark Labeling

**File:** `src/layouts/BaseLayout.astro`

**Problem:** Multiple `<nav>` elements need distinction.

**Fix:**
```html
<nav aria-label="Main navigation">
```

### 2. Section Headings

**File:** `src/pages/index.astro`

**Status:** ✅ Good - All sections have proper heading hierarchy (h1 → h2 → h3)

---

## Focus Management Issues

### 1. Theme Toggle Focus Ring

**Files:** `src/layouts/BaseLayout.astro`, `src/components/ThemeToggle.astro`

**Problem:** Theme toggle button needs visible focus indicator.

**Fix:**
```css
.theme-toggle:focus-visible {
  outline: 2px solid var(--color-accent);
  outline-offset: 2px;
  border-radius: 4px;
}
```

### 2. Modal/Dialog Focus Trap

**Status:** N/A - No modals on custom pages (Starlight search handles its own)

### 3. Accordion Focus

**File:** `src/components/CommandBuilder.astro`

**Status:** ✅ Native `<details>` elements handle focus correctly

---

## Positive Findings

The site does several things well:

1. ✅ **Semantic HTML** - Proper use of `<nav>`, `<main>`, `<footer>`, `<section>`
2. ✅ **Heading hierarchy** - Correct h1 → h2 → h3 structure
3. ✅ **Theme toggle** - Has dynamic `aria-label` in Starlight docs
4. ✅ **Color scheme** - Supports `prefers-color-scheme`
5. ✅ **Reduced motion** - Starlight respects `prefers-reduced-motion`
6. ✅ **Form labels** - Most inputs have associated labels
7. ✅ **Language attribute** - `<html lang="en">` is set
8. ✅ **Responsive design** - Works on mobile viewports
9. ✅ **Native details/summary** - Uses native accordion elements

---

## Priority Fixes

### High Priority (Should fix before launch)

1. Add skip link to main content
2. Add `aria-hidden="true"` to decorative elements
3. Add screen reader text for external links
4. Add `rel="noopener noreferrer"` to external links
5. Add focus indicators to interactive elements

### Medium Priority (Fix soon)

6. Add ARIA to Command Builder (radiogroup, aria-pressed)
7. Add accessible name to copy button
8. Label nav landmarks

### Low Priority (Nice to have)

9. Increase comment color contrast slightly
10. Platform-aware keyboard shortcuts
11. Add screen reader class for hidden text

---

## CSS Utilities Needed

Add to global styles:

```css
/* Screen reader only content */
.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border: 0;
}

/* Focus visible styles */
:focus-visible {
  outline: 2px solid var(--color-accent);
  outline-offset: 2px;
}

/* Respect reduced motion */
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    animation-duration: 0.01ms !important;
    animation-iteration-count: 1 !important;
    transition-duration: 0.01ms !important;
  }
}
```

---

## Testing Recommendations

1. **Automated testing:** Run axe-core or Lighthouse accessibility audits
2. **Keyboard testing:** Navigate entire site using only Tab, Enter, Escape, Arrow keys
3. **Screen reader testing:** Test with VoiceOver (macOS) or NVDA (Windows)
4. **Color blindness:** Use Sim Daltonism or similar to verify color usage
5. **Zoom testing:** Verify site works at 200% zoom

---

## Resources

- [WCAG 2.1 Guidelines](https://www.w3.org/WAI/WCAG21/quickref/)
- [WebAIM Contrast Checker](https://webaim.org/resources/contrastchecker/)
- [axe DevTools](https://www.deque.com/axe/devtools/)
- [Starlight Accessibility](https://starlight.astro.build/guides/accessibility/)
