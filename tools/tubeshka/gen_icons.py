# Draws the Tubeshka logo (the lavender disc of TGeshka with a white play sign) and writes every icon the app uses:
# assets/logo.png, assets/streamtube.ico and all assets/accent/* (the icon no longer follows the accent colour).
# Run: python tools/tubeshka/gen_icons.py   (requires Pillow)
import glob
import os

from PIL import Image, ImageDraw

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
ASSETS = os.path.join(ROOT, 'assets')

# TGeshka's gradient, top-left to bottom-right
TOP = (188, 170, 247)
BOTTOM = (132, 112, 222)
PLAY = (255, 255, 255)
SS = 8


def lerp(a, b, t):
    return tuple(int(round(x + (y - x) * t)) for x, y in zip(a, b))


def logo(size, margin=0.04):
    big = size * max(1, min(SS, 4096 // size))
    img = Image.new('RGBA', (big, big), (0, 0, 0, 0))
    inner = big - 2 * int(round(margin * big))
    off = (big - inner) // 2
    steps = 256
    grad = Image.new('RGBA', (steps, steps))
    px = grad.load()
    for y in range(steps):
        for x in range(steps):
            px[x, y] = lerp(TOP, BOTTOM, (x + y) / (2.0 * (steps - 1))) + (255,)
    grad = grad.resize((inner, inner), Image.BICUBIC)
    mask = Image.new('L', (inner, inner), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, inner - 1, inner - 1), fill=255)
    img.paste(grad, (off, off), mask)
    # play sign, slightly right of centre so it looks centred; corners rounded by drawing it fat and shrinking
    d = ImageDraw.Draw(img)
    s = inner / 100.0
    tri = [(38, 28), (38, 72), (76, 50)]
    pts = [(off + x * s, off + y * s) for x, y in tri]
    d.polygon(pts, fill=PLAY)
    r = 5 * s
    for x, y in pts:
        d.ellipse((x - r, y - r, x + r, y + r), fill=PLAY)
    d.line(pts + [pts[0]], fill=PLAY, width=int(2 * r))
    return img.resize((size, size), Image.LANCZOS)


def main():
    big = logo(256)
    big.save(os.path.join(ASSETS, 'logo.png'))
    sizes = [(16, 16), (20, 20), (24, 24), (32, 32), (40, 40), (48, 48), (64, 64), (128, 128), (256, 256)]
    big.save(os.path.join(ASSETS, 'streamtube.ico'), sizes=sizes)
    for f in glob.glob(os.path.join(ASSETS, 'accent', '*.png')):
        big.save(f)
    for f in glob.glob(os.path.join(ASSETS, 'accent', '*.ico')):
        big.save(f, sizes=sizes)
    svg = os.path.join(ASSETS, 'logo.svg')
    with open(svg, 'w', encoding='utf-8', newline='\n') as fh:
        fh.write('''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">
  <defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1">
    <stop offset="0" stop-color="#bcaaf7"/><stop offset="1" stop-color="#8470de"/></linearGradient></defs>
  <circle cx="50" cy="50" r="46" fill="url(#g)"/>
  <path d="M38.5 30.5 L38.5 69.5 L74 50 Z" fill="#fff" stroke="#fff" stroke-width="9" stroke-linejoin="round"/>
</svg>
''')


if __name__ == '__main__':
    main()
