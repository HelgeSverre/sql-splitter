import { useState } from 'react';

export default function SimpleCounter() {
  const [count, setCount] = useState(0);
  
  return (
    <div style={{ padding: '1rem', border: '1px solid #ccc', borderRadius: '8px', marginBlock: '1rem' }}>
      <p>Count: <strong id="counter-value">{count}</strong></p>
      <button 
        id="increment-btn"
        onClick={() => setCount(c => c + 1)}
        style={{ marginRight: '0.5rem', padding: '0.5rem 1rem' }}
      >
        Increment
      </button>
      <button 
        id="decrement-btn"
        onClick={() => setCount(c => c - 1)}
        style={{ padding: '0.5rem 1rem' }}
      >
        Decrement
      </button>
    </div>
  );
}
