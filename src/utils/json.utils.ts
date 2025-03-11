/**
 * Custom replacer function to convert BigInt and BigNumber
 * into a serializable form.
 */
export function customReplacer(key: string, value: any) {
  if (typeof value === 'bigint') {
    return { __type: 'BigInt', value: value.toString() };
  }
  if (value instanceof BigNumber) {
    return { __type: 'BigNumber', value: value.toString() };
  }
  return value;
}

/**
 * Custom reviver function to convert our serialized objects
 * back into BigInt and BigNumber.
 */
export function customReviver(key, value) {
  if (value && typeof value === 'object') {
    if (value.__type === 'BigInt') {
      return BigInt(value.value);
    }
    if (value.__type === 'BigNumber') {
      return new BigNumber(value.value);
    }
  }
  return value;
}