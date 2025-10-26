import os, time
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torchvision import datasets, transforms
from torchvision.models import resnet18, ResNet18_Weights

# ---------- CONFIG ----------
data_root = "data"     # ชี้ไปยังโฟลเดอร์ด้านบน
batch_size = 32
epochs = 10
lr = 1e-3
weight_decay = 1e-2
feature_extract = True  # True = แช่แข็ง backbone แล้วเทรนเฉพาะชั้นสุดท้าย
save_path = "cnn_best.pt"
num_workers = 4
# ----------------------------

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print("Device:", device)

# Transform ตามสไตล์ ImageNet
img_size = 224
train_tfms = transforms.Compose([
    transforms.RandomResizedCrop(img_size),
    transforms.RandomHorizontalFlip(),
    transforms.ToTensor(),
    transforms.Normalize(mean=ResNet18_Weights.IMAGENET1K_V1.transforms().mean,
                         std=ResNet18_Weights.IMAGENET1K_V1.transforms().std),
])
val_tfms = transforms.Compose([
    transforms.Resize(256),
    transforms.CenterCrop(img_size),
    transforms.ToTensor(),
    transforms.Normalize(mean=ResNet18_Weights.IMAGENET1K_V1.transforms().mean,
                         std=ResNet18_Weights.IMAGENET1K_V1.transforms().std),
])

train_dir = os.path.join(data_root, "train")
val_dir   = os.path.join(data_root, "val")

train_ds = datasets.ImageFolder(train_dir, transform=train_tfms)
val_ds   = datasets.ImageFolder(val_dir,   transform=val_tfms)

train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True,
                          num_workers=num_workers, pin_memory=True)
val_loader   = DataLoader(val_ds,   batch_size=batch_size, shuffle=False,
                          num_workers=num_workers, pin_memory=True)

num_classes = len(train_ds.classes)
print("Classes:", train_ds.classes)

# โหลด ResNet18 (CNN) ที่ pretrain มา
model = resnet18(weights=ResNet18_Weights.IMAGENET1K_V1)

# แช่แข็งพารามิเตอร์ทั้งหมดถ้า feature_extract = True
if feature_extract:
    for p in model.parameters():
        p.requires_grad = False

# แทนที่ชั้น fully-connected ให้ตรงกับจำนวนคลาสเรา
in_features = model.fc.in_features
model.fc = nn.Linear(in_features, num_classes)

model = model.to(device)

criterion = nn.CrossEntropyLoss()

# ออปติไมเซอร์จะอัปเดตเฉพาะพารามิเตอร์ที่ requires_grad=True
params_to_update = [p for p in model.parameters() if p.requires_grad]
optimizer = torch.optim.AdamW(params_to_update, lr=lr, weight_decay=weight_decay)
scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=epochs)

def run_epoch(loader, train=True):
    if train:
        model.train()
    else:
        model.eval()

    total_loss, total_correct, total = 0.0, 0, 0
    t0 = time.time()

    for imgs, labels in loader:
        imgs, labels = imgs.to(device, non_blocking=True), labels.to(device, non_blocking=True)

        if train:
            optimizer.zero_grad()

        with torch.set_grad_enabled(train):
            logits = model(imgs)
            loss = criterion(logits, labels)

            if train:
                loss.backward()
                optimizer.step()

        total_loss += loss.item() * imgs.size(0)
        preds = logits.argmax(1)
        total_correct += (preds == labels).sum().item()
        total += imgs.size(0)

    avg_loss = total_loss / total
    acc = total_correct / total
    return avg_loss, acc, time.time() - t0

best_acc = 0.0
for epoch in range(1, epochs + 1):
    tr_loss, tr_acc, tr_sec = run_epoch(train_loader, train=True)
    va_loss, va_acc, va_sec = run_epoch(val_loader,   train=False)
    scheduler.step()

    print(f"Epoch {epoch:02d}/{epochs} | "
          f"Train loss {tr_loss:.4f} acc {tr_acc:.4f} ({tr_sec:.1f}s) | "
          f"Val loss {va_loss:.4f} acc {va_acc:.4f} ({va_sec:.1f}s)")

    # เซฟเฉพาะโมเดลที่วาเลดีที่สุด
    if va_acc > best_acc:
        best_acc = va_acc
        torch.save({
            "state_dict": model.state_dict(),
            "classes": train_ds.classes,
            "img_size": img_size,
        }, save_path)
        print(f"  ✅ Saved best model to {save_path} (val_acc={best_acc:.4f})")

print("Done. Best val_acc:", best_acc)
